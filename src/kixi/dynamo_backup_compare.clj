(ns kixi.dynamo-backup-compare
  (:require [amazonica.aws.s3 :as s3]
            [baldr.core :as baldr]
            [cheshire.core :as json]
            [clj-time.core :as t]
            [clojure.data :refer [diff]]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [kixi.event-backup-to-event-log.file-size :refer [correct-file-size]]
            [kixi.event-backup-to-event-log.group-event-fixer
             :refer
             [correct-group-created-events]]
            [kixi.event-backup-to-event-log.old-format-parser :refer [file->events]]
            [kixi.event-backup-to-event-log.partition-keys
             :refer
             [event->partition-key]]
            [kixi.hour-sequence :refer [hour-sequence]]
            [kixi.maws :refer [witan-prod-creds witan-staging-creds]]
            [kixi.new-file-writer :refer [write-new-format]]
            [taoensso.nippy :as nippy])
  (:import [java.io ByteArrayInputStream File InputStream]))

(def max-objects 20)

(def credentials )

(defn bucket->s3-object-summaries
  ([credentials bucket prefix]
   (bucket->s3-object-summaries credentials bucket prefix nil))
  ([credentials bucket prefix marker]
   (let [list-objects-res (s3/list-objects credentials (merge {:bucket-name bucket
                                                               :prefix prefix
                                                               :max-keys max-objects}
                                                              (when marker
                                                                {:marker marker})))]
     (concat (:object-summaries list-objects-res)
                (when (:next-marker list-objects-res)
                  (bucket->s3-object-summaries credentials bucket prefix (:next-marker list-objects-res)))))))

(defn object-summary->local-file
  [credentials local-dir bucket s3-object-summary]
  (let [^File local-file (io/file local-dir (last (string/split (:key s3-object-summary) #"/")))]
    (when-not (.exists local-file)
      (.createNewFile local-file)
      (let [s3-object (s3/get-object credentials
                                     :bucket-name bucket
                                     :key (:key s3-object-summary))]
        (with-open [^InputStream in (:input-stream s3-object)]
          (io/copy in local-file))))
    local-file))


(def local-base-dir "./dynamo-backups/")

(def replay-base-dir (str local-base-dir "replay"))
(def prod-base-dir (str local-base-dir "prod"))

(def prod-s3
  {:local-dir prod-base-dir
   :region "eu-west-1"
   :credentials (assoc (witan-prod-creds)
                       :endpoint "eu-west-1")})

(def replay-s3
  {:local-dir replay-base-dir
   :region "eu-central-1"
   :credentials (assoc (witan-staging-creds)
                       :endpoint "eu-central-1")})

(def datastore-prod-backup (merge prod-s3
                                  {:bucket "prod.witan-dynamo-backup"
                                   :prefix "kixi.datastore/2017-11-20-04-27-27"}))

(def datastore-replay-backup (merge replay-s3
                                    {:bucket "staging.witan-dynamo-backup"
                                     :prefix "kixi.datastore/2017-11-20-17-16-58"}))

(def heimdall-prod-backup (merge prod-s3
                                 {:bucket "prod.witan-dynamo-backup"
                                  :prefix "kixi.heimdall/2017-11-20-04-26-20"}))

(def heimdall-replay-backup (merge replay-s3
                                   {:bucket "staging.witan-dynamo-backup"
                                    :prefix "kixi.heimdall/2017-11-24-15-15-59"}))


(defn download-backup
  [{:keys [credentials
           bucket
           local-dir
           region
           prefix]
    :as backup}]
  (doseq [obj (bucket->s3-object-summaries credentials bucket prefix)]
    (object-summary->local-file credentials local-dir bucket obj)))

(defn file->id-entities
  [file-name id-field]
  (let [entities (->> file-name
                      io/reader
                      json/parse-stream
                      vals
                      first
                      (map #(get-in % ["PutRequest" "Item"]))
                      (map #(zipmap (keys %)
                                    (map (comp second first) (vals %)))))]
    (reduce
     #(assoc %1
             (%2 id-field)
             %2)
     {}
     entities)))

(defn datastore-file->id-entries
  [file]
  (file->id-entities file "kixi.datastore.metadatastore_id"))

(defn heimdall-user-file->id-entries
  [file]
  (file->id-entities file "id"))

(defn load-backup-into-mem
  [base-dir prefix file->entities]
  (transduce
   (comp (filter #(.startsWith (.getName %) prefix))
         (map file->entities))
   merge
   (->> base-dir
        io/file
        file-seq
        rest)))


(def replay-metadatastore (load-backup-into-mem
                           replay-base-dir
                           "staging-replay-kixi.datastore-metadatastore-"
                           datastore-file->id-entries))
(def prod-metadatastore (load-backup-into-mem
                         prod-base-dir
                         "prod-kixi.datastore-metadatastore-"
                         datastore-file->id-entries))

(def replay-heimdall-user (load-backup-into-mem
                           replay-base-dir
                           "staging-replay-kixi.heimdall-user"
                           heimdall-user-file->id-entries))
(def prod-heimdall-user (load-backup-into-mem
                         prod-base-dir
                         "prod-kixi.heimdall-user"
                         heimdall-user-file->id-entries))

(def replay-heimdall-group (load-backup-into-mem
                            replay-base-dir
                            "staging-replay-kixi.heimdall-group"
                            heimdall-user-file->id-entries))
(def prod-heimdall-group (load-backup-into-mem
                          prod-base-dir
                          "prod-kixi.heimdall-group"
                          heimdall-user-file->id-entries))


(def replay-not-prod (remove (set (keys prod-metadatastore)) (keys replay-metadatastore)))
(def prod-not-replay (remove (set (keys replay-metadatastore)) (keys prod-metadatastore)))

(defn examine-row
  [r]
  (select-keys r ["kixi.datastore.metadatastore_name"
                  "kixi.datastore.metadatastore_description"
                  "kixi.datastore.metadatastore_provenance|kixi.datastore.metadatastore_created"
                  "kixi.datastore.metadatastore_provenance|kixi.user_id"
                  "kixi.datastore.metadatastore_sharing|kixi.datastore.metadatastore_meta-read"]))

(->> (select-keys prod-metadatastore prod-not-replay)
    vals
    (map #(select-keys % ["kixi.datastore.metadatastore_name"
                          "kixi.datastore.metadatastore_description"
                          "kixi.datastore.metadatastore_provenance|kixi.datastore.metadatastore_created"
                          "kixi.datastore.metadatastore_provenance|kixi.user_id"
                          "kixi.datastore.metadatastore_sharing|kixi.datastore.metadatastore_meta-read"])))

(filter #(= "Amlin Yacht Reports"
            (get-in % ["kixi.datastore.metadatastore_name"]))
        (vals prod-metadatastore))

(defn diff-backups
  [prod-set replay-set]
  (remove
   (fn [[a b c]]
     (and (nil? a)
          (nil? b)))
   (map
    (fn [id]
      (diff (get prod-set id)
            (get replay-set id)))
    (clojure.set/union (set (keys replay-set))
                       (set (keys prod-set))))))

(def datastore-metadata-diff
  (diff-backups prod-metadatastore
                replay-metadatastore))

(def heimdall-user-diff
  (diff-backups prod-heimdall-user
                replay-heimdall-user))
(def heimdall-group-diff
  (diff-backups prod-heimdall-group
                replay-heimdall-group))

(def seven-forgettable-files #{"bdd8b92e-c6df-4498-9ca4-a5778693f972"
                               "aa277eff-b130-4993-8ec9-d43a9fdb25c6"
                               "267b9ce0-0983-4675-bf0f-a8f71603f853"
                               "34fd5809-7444-4966-a18a-47a272e6407f"
                               "e1118499-d0f7-4eb4-a837-dc84e86fcc6b"
                               "62a4e3ca-726d-4e3a-919d-dbf69fd1d8a4"

                               "16f6efeb-58db-4ed5-888f-75df5ff1e1a6"})

(def remarkable-diffs (remove
                       (fn [[a b c]]
                         (and (nil? a)
                              (nil? b)))
                       (map
                        (fn [id]
                          (diff (get prod-metadatastore id)
                                (get replay-metadatastore id)))
                        (remove seven-forgettable-files
                                all-ids))))

                                        ;(clojure.pprint/pprint remarkable-diffs)
