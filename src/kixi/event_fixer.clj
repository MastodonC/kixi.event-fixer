(ns kixi.event-fixer
  (:gen-class)
  (:require [amazonica.aws.s3 :as s3]
            [baldr.core :as baldr]
            [clj-time.core :as t]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [kixi.group-event-fixer :refer [correct-group-created-events]]
            [kixi.hour-sequence :refer [hour-sequence]]
            [kixi.maws :refer [witan-prod-creds]]
            [kixi.new-file-writer :refer [write-new-format]]
            [kixi.old-format-parser :refer [file->events]]
            [kixi.partition-keys :refer [event->partition-key]]
            [kixi.file-size :refer [correct-file-size]]
            [taoensso.nippy :as nippy])
  (:import [java.io ByteArrayInputStream File InputStream]))

;; Run Ctrl-c Ctrl-k on the buffer to generate new credentials
(def credentials (assoc (witan-prod-creds) :client-config {:max-connections 50
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

(defn reshape-event
  [event]
  (if-not (:error event)
    (assoc event
           :partition-key (event->partition-key event))
    event))

(defn event->event-plus-sequence-num
  [xf]
  (let [sequence-num-seq (atom (range (Long/MAX_VALUE)))]
    (fn
      ([] (xf))
      ([acc] (xf acc))
      ([acc event]
       (let [num (first @sequence-num-seq)]
         (swap! sequence-num-seq rest)
         (xf acc (assoc event
                        :sequence-num num)))))))

(defn prn-t
  [x]
  (prn x)
  x)

(def backup-start-hour "2017-04-14T16")

;(def backups-old-format-end-hour "2017-10-16T12")
(def backups-old-format-end-hour "2017-10-18T12")

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
         (map (object-summary->local-file prod-backup-s3-base local-old-format-base-dir))
         file->events
         (keep correct-group-created-events)
         (map correct-file-size)
         event->event-plus-sequence-num
         (map reshape-event)
         (map (write-new-format local-new-format-base-dir)))
   counter
   (hour-sequence backup-start-hour
                  backups-old-format-end-hour)))

(defn valid-event?
  [event]
  (try
    (->> (update event
                 :event
                 nippy/thaw)
         :event
         (#(or :kixi.comms.event/key %
               :kixi.event/type %)))
    (catch Exception e
      false)))

(defn file->error-report
  [^File file]
  {(keyword (.getName file))
   (reduce
    (fn [report event]
      (if (valid-event? event)
        (update report :valid inc)
        (update report :error inc)))
    {:valid 0
     :error 0}
    (map
     nippy/thaw
     (baldr/baldr-seq (io/input-stream file))))})

(defn report-errors
  ([] {})
  ([acc] acc)
  ([acc x]
   (merge acc x)))

(defn validate-new-format-files
  []
  (transduce
   (comp (map file->error-report)
         (filter (fn [report]
                   (not (zero? (:error (first (vals report))))))))
   report-errors
   (rest
    (file-seq
     (io/file local-new-format-base-dir)))))

(defn peak-files
  []
  (->> (io/file "/home/tom/Documents/clojure/kixi.event-fixer/event-log/new-format")
       file-seq
       (map io/input-stream)
       (mapcat baldr/baldr-seq)
       (map nippy/thaw)
       (map #(update % :event nippy/thaw))
       ))
