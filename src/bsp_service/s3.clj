(ns bsp_service.s3
  (:import java.io.ByteArrayInputStream
           java.text.SimpleDateFormat
           java.util.Date)
  (:require [clojure.core.async :as async])
  (:use [amazonica.core]
        [amazonica.aws.s3]))

;; put object from stream
(defn upload
  [file-name byte-buffer buffer-len]
  (put-object :bucket-name "test-analytic-data"
              :key file-name
              :input-stream byte-buffer
              :metadata {:content-length buffer-len}
              :return-values "ALL_OLD"))


(defn upload-buffer
  [file-name string-buffer]
  (let [data (.getBytes(.toString string-buffer))]
    (upload file-name (ByteArrayInputStream. data) (count data))))


(defn now [] (new Date))
(def date-format (new SimpleDateFormat "yyyy-MM-dd-HH-ss-SS"))


(defn to-s3
  [storage-params]
  (fn [to-bucket group-name]

    ;Repeat until the channel returns nil (Closed)
    (loop []
      (if-let [log_chunk (async/<!! to-bucket)]
        (let [file-name (str  group-name "-"
                              (.format date-format (now)) "-"
                              (:instance-hash storage-params) ".txt")]
          (upload-buffer file-name log_chunk)
          (recur))
        (println "Closing down channel")))))