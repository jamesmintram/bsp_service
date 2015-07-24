;
; Code for buffering and pushing out log streams to S3
;
(ns bsp_service.core
  (:require [liberator.core :refer [resource defresource]]
            [ring.middleware.params :refer [wrap-params]]
            [compojure.core :refer [defroutes ANY]]
            [clojure.core.async :as async]
            [clojure.string :as string]))

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
    [group input-channel]
    (let [output-channel (async/chan)]
    (async/go (storage-buffer group input-channel output-channel))
    output-channel))


(defn start-channel-consumers
  [channel-map destination]
  (doseq [[group-name group-data] channel-map]
    (async/thread (destination group-name (storage-buffer-channel (group-data :data) (group-data :chan))))))


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
          (println "Event type not found - need to send down the error pipe")
          (do (async/>!! (event-channel :chan) event-pieces)))))))


(defn feed-channel-pipeline [event-source pipeline]
  (while true
    (let [event (async/<!! event-source)]
      (pipeline event))))


(defn now [] (new java.util.Date))
(def date-format (new java.text.SimpleDateFormat "yyyy-MM-dd-HH-ss-SS"))

(defn to-file
  [storage-params]
  (fn [group-name to-bucket]
    (println group-name)
    (doseq [i (iterate inc 1)]
      (let [file-name (str  group-name "-"
                            (.format date-format (now)) "-"
                            (:instance-hash storage-params) "-log.txt")
            log_chunk (async/<!! to-bucket)
            bwr (new java.io.BufferedWriter(new java.io.FileWriter (new java.io.File file-name)))]

        (.write bwr log_chunk)
        (.flush bwr)
        (.close bwr)))))


;Load this data in so we can throw out bad event types
(def event-types {
                  "http" {:buffer (* 1024 1)}
                  "ssh" {:buffer (* 1024 1)}
                  })

(def event-sink (async/chan (async/sliding-buffer 1024)))
(defn test_boot []
    (async/thread
     (feed-channel-pipeline event-sink
                            (create-channel-pipeline event-types
                                                     (to-file {:instance-hash "ABCD"})))))


(defroutes app
  (ANY "/" [] (resource :available-media-types ["text/html"]
                           :handle-ok "<html>Hello, Internet</html>"))

  (ANY "/foo" [] (resource :available-media-types ["text/html"]
                           :handle-ok (fn [ctx]
                                        (async/>!! event-sink "http|tasklist|error|ListEmpty")
                                        (format "<html>It's %d milliseconds since the beginning of the epoch."
                                                (System/currentTimeMillis))))))

(def handler
  (-> app
      wrap-params))
