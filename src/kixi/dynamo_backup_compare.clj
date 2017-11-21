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

(def backup-one {:local-dir (str local-base-dir "prod")
                 :bucket "prod.witan-dynamo-backup"
                 :region "eu-west-1"
                 :prefix "kixi.datastore/2017-11-20-04-27-27"
                 :credentials (assoc (witan-prod-creds)
                                     :endpoint "eu-west-1")})


(def backup-two {:local-dir (str local-base-dir "replay")
                 :bucket "staging.witan-dynamo-backup"
                 :region "eu-central-1"
                 :prefix "kixi.datastore/2017-11-20-17-16-58"
                 :credentials (assoc (witan-staging-creds)
                                     :endpoint "eu-central-1")})


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
  [file-name]
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
             (%2 "kixi.datastore.metadatastore_id")
             %2)
     {}
     entities)))

(defn load-backup-into-mem
  [base-dir prefix]
  (transduce
   (comp (filter #(.startsWith (.getName %) prefix))
         (map file->id-entities))
   merge
   (->> base-dir
        io/file
        file-seq
        rest)))


(def replay-metadatastore (load-backup-into-mem "./dynamo-backups/replay" "staging-replay-kixi.datastore-metadatastore-"))
(def prod-metadatastore (load-backup-into-mem "./dynamo-backups/prod" "prod-kixi.datastore-metadatastore-"))


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

(def all-ids (clojure.set/union (set (keys replay-metadatastore))
                                (set (keys prod-metadatastore))))


(def diffs (remove
            (fn [[a b c]]
              (and (nil? a)
                   (nil? b)))
            (map
             (fn [id]
               (diff (get prod-metadatastore id)
                     (get replay-metadatastore id)))
             all-ids)))

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

(clojure.pprint/pprint remarkable-diffs)
;; => nil


;;remarkable four investigations

(def remarkable-one "1d13be67-bb52-44c2-bc67-a7b26e670924")
;; => #'kixi.dynamo-backup-compare/remarkable-one

(get-in replay-metadatastore [remarkable-one "kixi.datastore.metadatastore_sharing|kixi.datastore.metadatastore_meta-read"])
;; => ["567ace3f-d531-4af2-ab5d-d8fe7f4e3079" "7d99af7c-691b-41a5-a1cb-8682a8a5eb1a"]

(get-in prod-metadatastore [remarkable-one "kixi.datastore.metadatastore_sharing|kixi.datastore.metadatastore_meta-read"])
;; => ["567ace3f-d531-4af2-ab5d-d8fe7f4e3079"]

;; An extra meta-read share in replay, but not in prod. Event processing failure?


(def remarkable-two "f73e8fe0-079d-4d3f-8670-779cf53d4c00")

(get-in replay-metadatastore [remarkable-two "kixi.datastore.metadatastore_logo"])
;; => nil

(get-in prod-metadatastore [remarkable-two "kixi.datastore.metadatastore_logo"])
;; => "https://data.london.gov.uk/wp-content/themes/theme_london/img/icon-publisher/gla.png"

;; Prod has a logo set. Replay incapable of processing an event prod has processed?


(def remarkable-three "7dbe7d1d-f893-49e5-868c-9962200c1506")

(get-in replay-metadatastore [remarkable-three "kixi.datastore.metadatastore_logo"])
;; => nil

(get-in prod-metadatastore [remarkable-three "kixi.datastore.metadatastore_logo"])
;; => "https://data.london.gov.uk/wp-content/themes/theme_london/img/icon-publisher/gla.png"

;; Same problem. Prod has logo.


  (def remarkable-four "891cd066-ddeb-43f7-bbe3-d854c663c4ad")

(get-in replay-metadatastore [remarkable-four])
;; => {"kixi.datastore.metadatastore_file-type" "xlsx", "kixi.datastore.metadatastore_provenance|kixi.datastore.metadatastore_source" "upload", "kixi.datastore.metadatastore_provenance|kixi.user_id" "65661685-6757-45cf-8048-1ad6ecb6f8bb", "kixi.datastore.metadatastore_id" "891cd066-ddeb-43f7-bbe3-d854c663c4ad", "kixi.datastore.metadatastore_header" true, "kixi.datastore.metadatastore_size-bytes" "error", "kixi.datastore.metadatastore_provenance|kixi.datastore.metadatastore_created" "20170614T111153.598Z", "kixi.datastore.metadatastore_name" "The output from Step 1 of the Data Combiner", "kixi.datastore.metadatastore_sharing|kixi.datastore.metadatastore_meta-read" ["c3101619-dee9-469a-abe2-2030bc3acaec" "d6650967-61e3-4145-bc0b-add0198ae4da"], "kixi.datastore.metadatastore_type" "stored", "kixi.datastore.metadatastore_description" "This is the excel file that is the output from the Data Combiner script found in Mastodon C's GitHub repository https://github.com/mastodonc/esc "}

(get-in prod-metadatastore [remarkable-four])
;; => nil

;; File exists in replay, but not in prod. Prod failed to process a create event?
