(ns bsp_service.core
  (:require [liberator.core :refer [resource defresource]]
            [ring.middleware.params :refer [wrap-params]]
            [compojure.core :refer [defroutes ANY]]
            [clojure.core.async :as async]))


(defn process
  [line]
  (for [ i (range 3) ]
    (str line i)))

(defn to-s3
  [group to-bucket]
  (async/thread
    (while true
      (let [log_chunk (async/<!! to-bucket)] ))))

(defn storage-consumer
  "This will fill a buffer until full. When full it will
  swap to a new buffer and schedule the old buffer to be 
  stored."
  [group from-processed-messages to-bucket]
  (async/thread
    (while true
      (let [buffer_size (atom 0)
            buffer (atom ())
            buffer_max (group :buffer)]

        (while (< @buffer_size buffer_max)
          (let [line (async/<!! from-processed-messages)]
              (swap! buffer_size + (count line))
              (swap! buffer conj line)))
        (async/>!! to-bucket @buffer)
        (println "Flushing Queuefor group: " (group :name))))))

(defn message-consumer
  "This is the method which will take a message apart per group."
  [group from-message to-buffer]
  (async/thread
    (while true
      (let [line (async/<!! from-message)
            data (process line)]
        (doseq [current_line data]
          (async/>!! to-buffer current_line))))))

(defn message-fan-out
  [input-channel output-channel-count]
  (let 
    [output-channels (repeat output-channel-count (async/chan (async/sliding-buffer 1024)))]
    (async/thread
        (while true
          (let [line (async/<!! input-channel)]
            (doseq [channel output-channels]
              (async/>!! channel line)))))
    output-channels))

(defn start-async-consumers
  "Start a consumer for each group which can process each message as it sees fit."
  [groups event-sink]
  (let 
    [output-channels (message-fan-out event-sink (count groups))]
    (doseq [[group input-channel] (map list groups output-channels)]
        (let 
          [buffer-chan (async/chan) bucket-chan (async/chan)]
          
          (println "Starting consumer for channel: " (group :name))
          
          (message-consumer group input-channel buffer-chan)
          (storage-consumer group buffer-chan bucket-chan)
          (to-s3 group bucket-chan)))))


(def event-sink (async/chan (async/sliding-buffer 1024)))
(defn test_boot
  []
  (let
    [groups [ {:name "seg1" :buffer 1024}
              {:name "seg2" :buffer 328}
              {:name "seg3" :buffer 512}
              {:name "seg4" :buffer 128}]]
    (start-async-consumers groups event-sink)))


(comment 
  chan-to-store (async/chan)
    chan-to-bucket (async/chan)

  (storage-consumer chan-to-store chan-to-bucket)
    (to-s3 chan-to-bucket))


(defroutes app
  (ANY "/" [] (resource :available-media-types ["text/html"]
                           :handle-ok "<html>Hello, Internet</html>"))

  (ANY "/foo" [] (resource :available-media-types ["text/html"]
                           :handle-ok (fn [ctx]
                                        (async/>!! event-sink "Hello From a Request")
                                        (format "<html>It's %d milliseconds since the beginning of the epoch."
                                                (System/currentTimeMillis))))))

(def handler 
  (-> app 
      wrap-params))