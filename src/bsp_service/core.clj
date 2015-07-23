(ns bsp_service.core
  (:require [liberator.core :refer [resource defresource]]
            [ring.middleware.params :refer [wrap-params]]
            [compojure.core :refer [defroutes ANY]]
            [clojure.core.async :as async]))

(defn storage-buffer
  "This will fill a buffer until full. When full it will
  swap to a new buffer and schedule the old buffer to be 
  stored.
  Change this to a Java Buffer which I can then dump"
  [group from-processed-messages to-bucket]
  (while true
    (let [buffer_size (atom 0)
          buffer (atom ())
          buffer_max (group :buffer)]

      (while (< @buffer_size buffer_max)
        (let [line (async/<!! from-processed-messages)]
            (swap! buffer_size + (count line))
            (swap! buffer conj line)))
      (async/>!! to-bucket @buffer)
      (println "Flushing Queuefor group: " (group :name)))))

(defn storage-buffer-channel
  "Takes an input channel, and returns an output channel. Data from the input channel
  is buffered until a flush is triggered by becomming full."
    [group input-channel]
    (let [output-channel (async/chan)]
    (async/thread (storage-buffer group input-channel output-channel))
    output-channel))

(defn message-fan-out
  "Takes a single input channel and returns a list of channels - each of which will
  recieve everything from the input channel."

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
  [groups event-sink aggregator destination]
  (let 
    [ output-channels (message-fan-out event-sink (count groups)) ]
    (doseq [[group input-channel] (map list groups output-channels)]
        (async/thread (destination group (storage-buffer-channel group (async/map< aggregator input-channel)))))))


(comment
  An ETL (Extract Transform Load) application. 

  TODO: Look at closing channels, docs for pipe good place to start.
        Refactor this to use Async Pipelines - Reason I haven't yet; this is a learning excercise.

  )
(defn process
  [line]
  (for [ i (range 300) ]
    (str line i)))

(defn to-s3
  [group to-bucket]
  (while true
    (let [log_chunk (async/<!! to-bucket)] )))

(def event-sink (async/chan (async/sliding-buffer 1024)))
(defn test_boot
  []
  (let
    [groups [ {:name "seg1" :buffer 1024}
              {:name "seg2" :buffer 328}
              {:name "seg3" :buffer 512}
              {:name "seg4" :buffer 128}]]
    (start-async-consumers groups event-sink process to-s3)))


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