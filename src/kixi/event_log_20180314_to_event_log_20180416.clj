(ns kixi.event-log-20180314-to-event-log-20180416
  (:require [clojure.java.io :as io]
            [baldr.core :as baldr]
            [taoensso.nippy :as nippy]
            [kixi.hour-sequence :refer [hour-sequence]]
            [kixi.hour->s3-object-summaries :refer :all]
            [kixi.event-backup-to-event-log.partition-keys :refer [event->partition-key]]
            [kixi.new-file-writer :refer [write-new-format event->delivery-file-name]]))

(defn fix-size-bytes
  [event]
  )

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

(defn fix-bad-event
  [event-wrapped]
  (clojure.walk/postwalk
   (fn [x] (if (and (vector? x) ;; [:kixi.datastore.metadatastore/size-bytes :error]
                    (= :kixi.datastore.metadatastore/size-bytes (first x))
                    (= :error (second x)))
             (do (println x
                          "id="(get-in event-wrapped [:event :kixi.comms.event/id])
                          "pk="(get event-wrapped :partition-key)
                          "key="(get-in event-wrapped [:event :kixi.comms.event/key]))
                 (update x 1 (constantly 16)))
             x)) event-wrapped))

(def backup-start-hour "2017-04-14T16") ;; don't change
(def backup-end-hour "2018-04-20T15")

(def s3-source-bucket "prod-witan-event-log-20180314")
(def s3-destination-bucket "prod-witan-event-log-20180416")
(def local-source-cache-dir (str "./event-log/" s3-source-bucket))
(def local-destination-cache-dir (str "./event-log/" s3-destination-bucket))

(defn download-s3-backups-and-transform
  []
  (transduce
   (comp (mapcat (hour->s3-object-summaries s3-source-bucket))
         (map (object-summary->local-file s3-source-bucket local-source-cache-dir))
         (mapcat (event-log->events local-source-cache-dir))
         (map thaw-wrapped-event)
         (map fix-bad-event) ;; <- fixing logic
         (map (write-new-format local-destination-cache-dir event->delivery-file-name)))
   counter
   (hour-sequence backup-start-hour
                  backup-end-hour)))
