(ns kixi.hour->s3-object-summaries
  (:require [amazonica.aws.s3 :as s3]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clj-time.core :as t]
            [kixi.maws :refer [witan-prod-creds]])
  (:import [java.io ByteArrayInputStream File InputStream]))

(def credentials
  (assoc (witan-prod-creds)
         :client-config {:max-connections 50
                         :connection-timeout 5000
                         :socket-timeout 5000}))

(defn hour->s3-prefix
  [hour]
  (->> [(t/year hour)
        (t/month hour)
        (t/day hour)
        (t/hour hour)]
       (map str)
       (map #(if (= 1 (count %))
               (str "0" %)
               %))
       (interpose "/")
       (apply str)))

(def max-objects 20)

(defn hour->s3-object-summaries
  ([s3-base-dir]
   (fn [hour]
     (hour->s3-object-summaries s3-base-dir
                                (hour->s3-prefix hour)
                                nil)))
  ([s3-base-dir prefix marker]
   (let [list-objects-res (s3/list-objects credentials (merge {:bucket-name s3-base-dir
                                                               :prefix prefix
                                                               :max-keys max-objects}
                                                              (when marker
                                                                {:marker marker})))]
     (concat (:object-summaries list-objects-res)
             (when (:next-marker list-objects-res)
               (hour->s3-object-summaries s3-base-dir prefix (:next-marker list-objects-res)))))))



(defn object-summary->local-file
  [s3-base-dir local-base-dir]
  (fn
    [s3-object-summary]
    (let [^File local-file (io/file local-base-dir (last (string/split (:key s3-object-summary) #"/")))]
      (when-not (.exists local-file)
        (do (.createNewFile local-file)
            (let [s3-object (s3/get-object credentials
                                           :bucket-name s3-base-dir
                                           :key (:key s3-object-summary))]
              (with-open [^InputStream in (:input-stream s3-object)]
                (io/copy in local-file)))))
      local-file)))
