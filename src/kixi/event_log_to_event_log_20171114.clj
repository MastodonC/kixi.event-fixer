(ns kixi.event-log-to-event-log-20171114
  (:require [clojure.java.io :as io]
            [baldr.core :as baldr]
            [taoensso.nippy :as nippy]
            [kixi.hour-sequence :refer [hour-sequence]]
            [kixi.hour->s3-object-summaries :refer :all]
            [kixi.event-backup-to-event-log.partition-keys :refer [event->partition-key]]
            [kixi.new-file-writer :refer [write-new-format event->delivery-file-name]]))

(defn counter
  ([] 0)
  ([acc] acc)
  ([acc x]
   (inc acc)))

(defn event-log->events
  "Produces a stream of all events from new format log directory"
  [log-dir]
  (fn [file]
    (map #(assoc % :file file)
         (map nippy/thaw
              (with-open [in (io/input-stream file)]
                (doall (baldr/baldr-seq in)))))))

(defn thaw-wrapped-event
  [event-wrapped]
  (update event-wrapped
          :event
          nippy/thaw))

(defn reset-partition-key
  [event-wrapped]
  (assoc event-wrapped
         :partition-key
         (event->partition-key event-wrapped)))

(def backup-start-hour "2017-04-14T16")
(def backup-end-hour "2017-11-20T11")

(def prod-backup-s3-base "prod-witan-event-log")
(def local-old-format-base-dir "./event-log/old-format")
(def local-new-format-base-dir "./event-log2/new-format")

(defn download-s3-backups-and-transform
  []
  (transduce
   (comp (mapcat (hour->s3-object-summaries prod-backup-s3-base))
         (map (object-summary->local-file prod-backup-s3-base local-old-format-base-dir))
         (mapcat (event-log->events local-old-format-base-dir))
         (map thaw-wrapped-event)
         (map reset-partition-key)
         (map (write-new-format local-new-format-base-dir event->delivery-file-name)))
   counter
   (hour-sequence backup-start-hour
                  backup-end-hour)))
