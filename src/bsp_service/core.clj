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
            [bsp_service.s3 :as s3]))

; TODO: A control line wich will let us trigger a flush
; on the buffer
(defn storage-buffer
  "This will fill a buffer until full. When full it will
  swap to a new buffer and schedule the old buffer to be
  stored.
  Change this to a Java Buffer which I can then dump"
  [group from-processed-messages to-bucket]
  (while true
    (let [buffer_max (group :buffer)
          sbuffer (new StringBuffer)]

      (while (< (.length sbuffer) buffer_max)
        (let [item (async/<!! from-processed-messages)]
          (.append sbuffer (str item "\n"))))
      (async/>!! to-bucket (.toString sbuffer)))))

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
       (destination group-name)))))


(defn create-channel-map
  [segments]
  (let
    [channel-map (into {}
                       (for [[segment-name segment-data] segments]
                         [segment-name {:data segment-data :chan (async/chan)}]))]
    channel-map))


(defn create-channel-pipeline [event-types destination]
  (let
    [channel-map (create-channel-map event-types)]
    (start-channel-consumers channel-map destination )
    (fn [event]
      (let [[event-type app-id & event-pieces] (string/split event #"\|")
            event-channel (channel-map event-type)]
        (if (nil? event-channel)
          (println "Event type not found - need to send down the error pipe: " event-type)
          (do (async/>!! (event-channel :chan) event-pieces)))))))


(defn feed-channel-pipeline [event-source pipeline]
  (while true
    (let [event (async/<!! event-source)]
      (pipeline event))))


(defn now [] (new Date))
(def date-format (new SimpleDateFormat "yyyy-MM-dd-HH-ss-SS"))


(defn new-uuid
  "Retrieve a type 4 (pseudo randomly generated) UUID.
  The UUID is generated using a cryptographically
  strong pseudo random number generator."
  []
  (str (UUID/randomUUID)))


(defn to-file
  [storage-params]
  (fn [to-bucket group-name]
    (println group-name)
    (doseq [_ (iterate inc 1)]
      (let [file-name (str  group-name "-"
                            (.format date-format (now)) "-"
                            (:instance-hash storage-params) ".txt")
            log_chunk (async/<!! to-bucket)
            bwr (new BufferedWriter(new FileWriter (new File file-name)))]

        (.write bwr log_chunk)
        (.flush bwr)
        (.close bwr)))))


;Load this data in so we can throw out bad event types
(def event-types {
                  "http" {:buffer (* 1024 1024 10)}
                  "ssh" {:buffer (* 1024 1024 10)}
                  })


(def event-sink (async/chan (async/sliding-buffer 1024)))
(defn test_boot []
  (let [session-id (new-uuid)]
  (async/thread
   (feed-channel-pipeline event-sink
                          (create-channel-pipeline event-types
                                                   (to-file {:instance-hash session-id}))))))


(defroutes thin-app
  (GET "/" [] "")
  (POST "/foo" req (async/>!! event-sink (slurp (:body req))) ""))

(def handler
  (-> thin-app
      wrap-params))
