(ns bsp_service.core
  (:require [liberator.core :refer [resource defresource]]
            [ring.middleware.params :refer [wrap-params]]
            [compojure.core :refer [defroutes ANY]]
            [clojure.core.async :as async :refer :all]))


(defn process
  [line]
  (for [ i (range 3) ]
    (str line i)))

(defn to-s3
  [group to-bucket]
  (thread
    (while true
      (let [log_chunk (<!! to-bucket)] ))))

(defn storage-consumer
  "This will fill a buffer until full. When full it will
  swap to a new buffer and schedule the old buffer to be 
  stored."
  [group from-processed-messages to-bucket]
  (thread
    (while true
      (let [buffer_size (atom 0)
            buffer (atom ())
            buffer_max (group :buffer)]

        (while (< @buffer_size buffer_max)
          (let [line (<!! from-processed-messages)]
              (swap! buffer_size + (count line))
              (swap! buffer conj line)))
        (>!! to-bucket @buffer)
        (println "Flushing Queuefor group: " (group :name))))))

(defn message-consumer
  "This is the method which will take a message apart per group."
  [group from-message to-buffer]
  (thread
    (while true
      (let [line (<!! from-message)
            data (process line)]
        (doseq [current_line data]
          (>!! to-buffer current_line))))))

(defn message-fan-out
  [input-channel output-channel-count]
  (thread
      (while true
        (let [line (<!! from-message)]
          (doseq [current_line data]
            (>!! to-buffer current_line))))))

(defn start-async-consumers
  "Start a consumer for each group which can process each message as it sees fit."
  [groups from-message]
  (let 
    [output-channels (message-fan-out (count groups))]
    (doseq [[group input-channel] (map list groups output-channels)]
        (let 
          [buffer-chan (chan) bucket-chan (chan)]

          (println "Starting consumer for channel: " (group :name))

          (message-consumer group input-channel buffer-chan)
          (storage-consumer group buffer-chan bucket-chan)
          (to-s3 group bucket-chan)))))


(def event-sink (chan))
(defn test_boot
  []
  (let
    [chan-to-process event-sink
    groups [{:name "seg1" :buffer 1024} {:name "seg2" :buffer 328}]]
    (start-async-consumers groups event-sink)))


(comment 
  chan-to-store (chan)
    chan-to-bucket (chan)

  (storage-consumer chan-to-store chan-to-bucket)
    (to-s3 chan-to-bucket))


(defroutes app
  (ANY "/" [] (resource :available-media-types ["text/html"]
                           :handle-ok "<html>Hello, Internet</html>"))

  (ANY "/foo" [] (resource :available-media-types ["text/html"]
                           :handle-ok (fn [ctx]
                                        (>!! event-sink "Hello From a Request")
                                        (format "<html>It's %d milliseconds since the beginning of the epoch."
                                                (System/currentTimeMillis))))))

(def handler 
  (-> app 
      wrap-params))