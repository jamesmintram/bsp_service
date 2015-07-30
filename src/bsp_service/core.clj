;
; Code for buffering and pushing out log streams to S3
;
(ns bsp_service.core
  (:import java.util.Date
           java.text.SimpleDateFormat
           java.io.BufferedWriter
           java.io.FileWriter
           java.io.File
           java.util.UUID [randomUUID] )

  (:require [ring.middleware.params :refer [wrap-params]]
            [compojure.core :refer [defroutes ANY GET POST]]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [cheshire.core :as json]
            [bsp_service.s3 :as s3]))

; TODO: A control line wich will let us trigger a flush
; on the buffer
(defn storage-buffer
  "This will fill a buffer until full. When full it will
  swap to a new buffer and schedule the old buffer to be
  stored."
  [group from-processed-messages to-bucket]
  (loop []
    (let [buffer_max (group :buffer)
          sbuffer (new StringBuffer)]

      (loop []
        (if-let [item (async/<!! from-processed-messages)]
          ;Success in taking from the channel
          (do
            (.append sbuffer (str item "\n"))
            (if (< (.length sbuffer) buffer_max) (recur)))

          ;Failure in taking from the channel - flush and close
          (do
            (async/>!! to-bucket (.toString sbuffer))
            (async/close! to-bucket))))

      ;Attempt to flush - if we fail, then the channel has been closed
      (if (async/>!! to-bucket (.toString sbuffer))
        (recur)))))

(defn storage-buffer-channel
  "Takes an input channel, and returns an output channel. Data from the input channel
  is buffered until a flush is triggered by becomming full."
  [input-channel group]
  (let [output-channel (async/chan)]
    (async/go (storage-buffer group input-channel output-channel))
    output-channel))


(defn start-channel-consumers
  [channel-map destination]
  (doseq [[group-name group-data] channel-map]

     (async/thread
      (->
       (group-data :chan)
       (storage-buffer-channel (group-data :data))
       (destination (name group-name))))))


(defn create-channel-map
  [segments]
  (into {}
        (for [[segment-name segment-data] segments]
          [segment-name {:data segment-data :chan (async/chan)}])))


(defn create-channel-pipeline [event-types destination]
  (let
    [channel-map (create-channel-map event-types)]
    (start-channel-consumers channel-map destination )
    (fn [event]
      (let [[event-type-name app-id & event-pieces] (string/split event #"\|")
            event-type (keyword event-type-name)
            event-channel (channel-map event-type)]
        (if (nil? event-channel)
          (println "Event type not found - need to send down the error pipe: " event-type)
          (do (async/>!! (event-channel :chan) event-pieces)))))))


(defn feed-channel-pipeline [event-source pipeline]
  "Reads input from event-source until nil is encountered,
  indicating a closed channel."
  (loop []
    (when-let [event (async/<!! event-source)]
      (do (pipeline event) (recur))))(println "Closing Feed Channel"))


(defn now [] (new Date))
(def date-format (new SimpleDateFormat "yyyy-MM-dd-HH-ss-SS"))


(defn new-uuid
  "Retrieve a type 4 (pseudo randomly generated) UUID.
  The UUID is generated using a cryptographically
  strong pseudo random number generator."
  []
  (str (UUID/randomUUID)))

;Macrofy this - we just want the inner let expression
(defn to-file
  [storage-params]
  (fn [to-bucket group-name]

    ;Repeat until the channel returns nil (Closed)
    (loop []
      (if-let [log_chunk (async/<!! to-bucket)]
        (let [file-name (str  group-name "-"
                              (.format date-format (now)) "-"
                              (:instance-hash storage-params) ".txt")
              bwr (new BufferedWriter(new FileWriter (new File file-name)))]

          (.write bwr log_chunk)
          (.flush bwr)
          (.close bwr)
          (recur))
        (println "Closing down channel")))))


(defn int-or-default [default value] (if (integer? value) value default))

(def conf-processors [
                      [:buffer (partial int-or-default (* 1024 1024 10))]
                      [:buffer-2 (partial int-or-default (* 1024 1024 20))]
                    ])

(defn event-process
  [processors]
  (fn [event-conf]
    (into event-conf
          (for [[default-key default-item-fn] processors]
            (let [event-item (event-conf default-key)]
              [default-key (default-item-fn event-item)])))))

(defn process-config [config]
  (let [processors (event-process conf-processors)]
    (into {} (for [[key data] config] [key (processors data)]))))

(defn file-event-types []
  (process-config (json/parse-stream (clojure.java.io/reader "config.json") true)))

(def event-sink (async/chan 1024))

(defn test_boot []
  (let [session-id (new-uuid)]
   (async/thread
   (feed-channel-pipeline event-sink
   (create-channel-pipeline (file-event-types)
                           (to-file {:instance-hash session-id}))))))

(def RESPONSE_OK {:status 202
      :headers {}
      :body ""})

(def RESPONSE_BUSY {:status 503
                  :headers {}
                  :body ""})

(defroutes thin-app
  (GET "/" [] "")
           (POST "/foo" req
             (if (= [:failed :default] (async/alts!! [[event-sink (slurp (:body req))]] :default :failed))
                RESPONSE_BUSY
                RESPONSE_OK)))

(def handler
  (-> thin-app
      wrap-params))
