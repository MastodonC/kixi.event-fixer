(ns kixi.uploader
  (:require [amazonica.aws.s3 :as s3]
            [kixi.maws :refer [witan-admin-prod-creds]]
            [clojure.java.io :as io]
            [clojure.string :as string])
  (:import [java.io ByteArrayInputStream File InputStream]))

(def credentials (delay (assoc (witan-admin-prod-creds "058197")
                               :client-config {:max-connections 50
                                               :connection-timeout 5000
                                               :socket-timeout 5000})))

(def target-bucket "prod-witan-event-log-20180314")
(def local-new-history-dir (str "./event-log/" target-bucket))

(def file-name-matcher (partial re-matcher #"prod-witan-event-delivery-\d-(\d{4})-(\d\d)-(\d\d)-(\d\d).*"))

(defn determine-prefix
  [^File file]
  (let [name (.getName file)
        groups (re-find (file-name-matcher name))
        prefix (->> groups
                    rest
                    (interpose "/")
                    (apply str))]
    [prefix
     file]))

(defn upload-file
  [[prefix ^File file]]
  (try
    (s3/put-object
     @credentials
     :bucket-name target-bucket
     :key (str prefix "/" (.getName file))
     :file file)
    {:success true}
    (catch Exception e
      {:success false
       :exception e})))

(defn upload-files
  []
  (into []
        (comp (map determine-prefix)
              (map upload-file)
              (remove :success))
        (rest
         (file-seq
          (io/file local-new-history-dir)))))
