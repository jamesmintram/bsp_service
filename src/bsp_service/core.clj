(ns bsp_service.core
  (:require [liberator.core :refer [resource defresource]]
            [ring.middleware.params :refer [wrap-params]]
            [compojure.core :refer [defroutes ANY]]
            [clojure.core.async :as async]))

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
        (doseq [item (async/<!! from-processed-messages)]
          (.append sbuffer (str item "\n"))))
        

      (async/>!! to-bucket (.toString sbuffer)))))

(defn storage-buffer-channel
  "Takes an input channel, and returns an output channel. Data from the input channel
  is buffered until a flush is triggered by becomming full."
    [group input-channel]
    (let [output-channel (async/chan)]
    (async/go (storage-buffer group input-channel output-channel))
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
  "Start a consumer for each group which can process each message as it sees fit. Creates 1 thread
  per destination."
  [groups event-sink aggregator destination]
  (let 
    [ output-channels (message-fan-out event-sink (count groups)) ]
    (doseq [[group input-channel] (map list groups output-channels)]
        (async/thread (destination group (storage-buffer-channel group (async/map< aggregator input-channel)))))))


;  An ETL (Extract Transform Load) application. 
;
;  TODO Look at closing channels, docs for pipe good place to start.
;        Refactor this to use Async Pipelines - Reason I haven't yet; this is a learning excercise.
;
; Thread count is nSegments + 1
;
(defn process
  [line]
  (for [ i (range 3) ]
    (str line i)))

(defn now [] (new java.util.Date))
(def date-format (new java.text.SimpleDateFormat "yyyy-MM-dd-HH-ss-SS"))

(defn to-s3
  [storage-params]
  (fn [group to-bucket]
    (doseq [i (iterate inc 1)]
      (let [file-name (str  (:name group) "-" 
                            (.format date-format (now)) "-" 
                            (:instance-hash storage-params) "-log.txt")
            log_chunk (async/<!! to-bucket)
            bwr (new java.io.BufferedWriter(new java.io.FileWriter (new java.io.File file-name)))] 

        (.write bwr log_chunk)
        (.flush bwr)
        (.close bwr)))))
 
;        (println "DUMP:" (.format date-format (now)) i log_chunk)))))

(def event-sink (async/chan (async/sliding-buffer 1024)))
(defn test_boot
  []
  (let
    [groups [ {:name "seg1" :buffer 1024}
              {:name "seg2" :buffer 328}
              {:name "seg3" :buffer 512}
              {:name "seg4" :buffer 128}]]
    (start-async-consumers groups event-sink process (to-s3 {:instance-hash "ABCD"}))))


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