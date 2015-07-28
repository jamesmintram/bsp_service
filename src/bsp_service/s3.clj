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
    (println group-name)
    (doseq [_ (iterate inc 1)]
      (let [file-name (str  group-name "-"
                            (.format date-format (now)) "-"
                            (:instance-hash storage-params) ".txt")
            log_chunk (async/<!! to-bucket)]
        ;Write out the buffer to S3
        (println "Uploading to S3")
        (upload-buffer file-name log_chunk)
        ))))
