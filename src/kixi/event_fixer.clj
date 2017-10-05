(ns kixi.event-fixer
  (:require [aero.core :as aero]
            [amazonica.aws.s3 :as s3]
            [amazonica.core :as amazonica]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.periodic :as p]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [cognitect.transit :as transit]
            [kixi.old-format-parser :refer [file->events]])
  (:import [java.io File]
           [java.io ByteArrayInputStream]))

(def one-hour (t/hours 1))

(def datehour-formatter
  (f/formatter :date-hour))

(def parse-datahour
  (partial f/parse datehour-formatter))

(def unparse-datahour
  (partial f/unparse datehour-formatter))

(defn hour-sequence
  [start-datehour end-datehour]
  (let [end-datehour (or end-datehour
                         (unparse-datahour (t/now)))
        start (parse-datahour start-datehour)
        end (parse-datahour end-datehour)]
    (p/periodic-seq start
                    (t/plus end
                            one-hour)
                    one-hour)))

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
   (let [list-objects-res (s3/list-objects (merge {:bucket-name s3-base-dir
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
    (let [s3-object (s3/get-object :bucket-name s3-base-dir
                                   :key (:key s3-object-summary))
          ^File local-file (io/file local-base-dir (last (string/split (:key s3-object-summary) #"/")))]
      (when-not (.exists local-file)
        (do (.createNewFile local-file)
            (io/copy (:object-content s3-object)
                     local-file)))
      local-file)))





(defn uuid
  [_]
  (str (java.util.UUID/randomUUID)))

(def event-type-version->partition-key-fn
  {[:kixi.datastore.file-metadata/updated "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])
   [:kixi.datastore.filestore/upload-link-created "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.filestore//id])
   [:kixi.heimdall/user-logged-in "1.0.0"] uuid
   [:kixi.datastore.file/created "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.filestore//id])
   [:kixi.datastore.metadatastore/update-rejected "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])
   [:kixi.datastore.filestore/download-link-created "1.0.0"] #(get-in % [:kixi.comms.event/payload :kixi.datastore.metadatastore/id])
   [:kixi.heimdall/group-created "1.0.0"] #(get-in % [:kixi.comms.event/payload :user-id])
   [:kixi.heimdall/user-created "1.0.0"] uuid
   [:kixi.heimdall/invite-created "1.0.0"] uuid
   [:kixi.mailer/mail-accepted "1.0.0"] uuid
   })

(def event->event-type-version
  (juxt #(or (:kixi.event/type %) (:kixi.comms.event/key %))
        #(or (:kixi.event/version %) (:kixi.comms.event/version %))))

(defn new-format-datastore-event?
  [event]
  (some-> event
          :kixi.event/type
          namespace
          (= "kixi.datastore")))

(defn event->partition-key
  [{:keys [event]}]
  (if-let [partition-key-fn (->> event
                                 event->event-type-version
                                 event-type-version->partition-key-fn)]
    (partition-key-fn event)
    (if (new-format-datastore-event? event)
      (if-let [partition-key (or (:kixi.datastore.metadatastore/id event)
                                 (:kixi.datastore.schemastore/id event))]
        partition-key
        (prn "Unknown event type: " (event->event-type-version event) "-" event))
      (prn "Unknown event type: " (event->event-type-version event) "-" event))))


(defn reshape-event
  [event]
  (if-not (:error event)
    (assoc event
           :partition-key (event->partition-key event)
           :dependencies {:transit "0.8.300"
                          :cheshire "5.7.0"})
    event))

(defn event->event-plus-sequence-num
  [xf]
  (let [sequence-num-seq (atom (range (Long/MAX_VALUE)))]
    (fn
      ([] (xf))
      ([acc] acc)
      ([acc event]
       (let [num (first @sequence-num-seq)]
         (swap! sequence-num-seq rest)
         (xf acc (assoc event
                        :sequence-num num)))))))


(defn prn-t
  [x]
  (prn x)
  x)


(defn event->file-name
  [local-base-dir
   {:keys [^File file] :as event}]
  (apply str local-base-dir "/" (.getName file)))

(defn write-new-format
  [local-base-dir]
  (fn [event]
    (let [file (io/file (event->file-name local-base-dir event))]
      (when-not (.exists file)
        (.createNewFile file))
      (with-open [out (io/output-stream file :append true)]
        (if-not (:error event)
          (transit/write (transit/writer out :json)
                         (select-keys event
                                      [:event
                                       :sequence-num
                                       :partition-key
                                       :dependencies]))
          (spit file (str event) :append true)))
      (spit file "\n" :append true))))


(def backup-start-hour "2017-04-29T16")

;(def backups-old-format-end-hour "2017-10-04T09")

(def backups-old-format-end-hour "2017-05-18T18")

(def staging-backup-s3-base "staging-witan-event-backup")
(def prod-backup-s3-base "prod-witan-event-backup")

(def local-old-format-base-dir "./event-log/old-format")
(def local-new-format-base-dir "./event-log/new-format")

(defn counter
  ([] 0)
  ([acc] acc)
  ([acc x]
   (inc acc)))

(defn download-s3-backups-and-transform
  []
  (transduce
   (comp (mapcat (hour->s3-object-summaries prod-backup-s3-base))
         (map prn-t)
         (map (object-summary->local-file prod-backup-s3-base local-old-format-base-dir))
         file->events
         event->event-plus-sequence-num
         (map reshape-event)
         (map (write-new-format local-new-format-base-dir)))
   counter
   (hour-sequence backup-start-hour
                  backups-old-format-end-hour)))






(defn valid-event-line?
  [^String encoded-str]
  (try
    (transit/read
     (transit/reader
      (ByteArrayInputStream. (.getBytes encoded-str))
      :json))
    true
    (catch Exception e
      false)))

(defn file->error-report
  [^File file]
  {(keyword (.getName file))
   (reduce
    (fn [report event-line]
      (if (valid-event-line? event-line)
        (update report :valid inc)
        (update report :error inc)))
    {:valid 0
     :error 0}
    (line-seq (io/reader file)))})

(defn report-errors
  ([] {})
  ([acc] acc)
  ([acc x]
   (merge acc x)))

(defn validate-new-format-files
  []
  (transduce
   (map file->error-report)
   report-errors
   (rest
    (file-seq
     (io/file local-new-format-base-dir)))))
