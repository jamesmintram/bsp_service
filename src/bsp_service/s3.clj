(ns bsp_service.s3
  (:import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
           org.joda.time.DateTime
           java.io.BufferedInputStream
           java.io.File
           java.io.FileInputStream
           java.text.SimpleDateFormat
           java.util.Date
           java.util.UUID
           java.security.KeyPairGenerator
           java.security.SecureRandom)
  (:require [clojure.string :as str]
            [clojure.core.async :as async :refer [<!!]])
  (:use ;[clojure.test]
        ;[clojure.pprint]
        [amazonica.core]
        [amazonica.aws.s3]))

;; put object from stream
(defn upload
  [file-name byte-buffer buffer-len]
  (put-object   cred
              :bucket-name "test-analytic-data"
              :key file-name
              :input-stream byte-buffer
              :metadata {:content-length buffer-len}
              :return-values "ALL_OLD"))


(defn upload-buffer
  [file-name string-buffer]
  (let [data (.getBytes(.toString string-buffer))]
    (upload file-name (java.io.ByteArrayInputStream. data) (count data))))

(defn now [] (new java.util.Date))
(def date-format (new java.text.SimpleDateFormat "yyyy-MM-dd-HH-ss-SS"))

(defn to-s3
  [storage-params]
  (fn [group-name to-bucket]
    (println group-name)
    (doseq [i (iterate inc 1)]
      (let [file-name (str  group-name "-"
                            (.format date-format (now)) "-"
                            (:instance-hash storage-params) ".txt")
            log_chunk (<!! to-bucket)]
        ;Write out the buffer to S3
        (println "Uploading to S3")
        (upload-buffer file-name log_chunk)
        ))))
