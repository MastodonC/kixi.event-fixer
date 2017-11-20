(ns kixi.event-backup-to-event-log
  (:gen-class)
  (:require [amazonica.aws.s3 :as s3]
            [baldr.core :as baldr]
            [clj-time.core :as t]
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
            [kixi.event-backup-to-event-log.event-filter :refer [unwanted-event?]]
            [kixi.hour-sequence :refer [hour-sequence]]
            [kixi.maws :refer [witan-prod-creds]]
            [kixi.new-file-writer :refer [write-new-format]]
            [kixi.hour->s3-object-summaries :refer [hour->s3-object-summaries
                                                    object-summary->local-file]]
            [taoensso.nippy :as nippy])
  (:import [java.io ByteArrayInputStream File InputStream])  )

(comment "Contains the repl trigger function for transforming the event log from 'prod-witan-event-backup' into the log in 'prod-witan-event-log'.")


(defn reshape-event
  [event]
  (if-not (:error event)
    (if-let [pkey (event->partition-key event)]
      (if-not (empty? pkey)
        (assoc event
               :partition-key pkey)
        (throw (ex-info "EMPTY Pkey" event)))
      (throw (ex-info "No Pkey" event)))
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
         (remove unwanted-event?)
         event->event-plus-sequence-num
         (map reshape-event)
         (map (write-new-format local-new-format-base-dir)))
   counter
   (hour-sequence backup-start-hour
                  backups-old-format-end-hour)))

(def prod-new-format-s3-base "prod-witan-event-log")
(def complete-new-format-log-dir "./event-log/complete-new-log")

(defn download-complete-new-log
  []
  (transduce
   (comp (mapcat (hour->s3-object-summaries prod-new-format-s3-base))
         (map (object-summary->local-file prod-new-format-s3-base
                                          complete-new-format-log-dir)))
   counter
   (hour-sequence "2017-11-03T10"
                  "2017-11-03T23")))
