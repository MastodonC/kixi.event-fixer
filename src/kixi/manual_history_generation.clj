(ns kixi.manual-history-generation
  "Untility fns for transforming json files into new format event log files."
  (:require [amazonica.aws.s3 :as s3]
            [baldr.core :as baldr]
            [clj-time.core :as t]
            [clojure.java.io :as io]
            [clojure.data :refer [diff]]
            [cheshire.core :as json]
            [clojure.string :as string]
            [kixi.group-event-fixer :refer [correct-group-created-events]]
            [kixi.hour-sequence :refer [hour-sequence]]
            [kixi.maws :refer [witan-prod-creds witan-staging-creds]]
            [kixi.new-file-writer :refer [write-new-format]]
            [kixi.old-format-parser :refer [file->events]]
            [kixi.partition-keys :refer [event->partition-key]]
            [kixi.file-size :refer [correct-file-size]]
            [taoensso.nippy :as nippy]))


(defn prep-events
  [events]
  (->> events
       (map #(hash-map :event %))
       (map (fn [s e]
              (assoc e
                     :partition-key (event->partition-key e)
                     :sequence-num s))
            (range 1 1000))))

(defn keyword-update-type
  [event]
  (if (= :kixi.datastore.file-metadata/updated (:kixi.comms.event/key event))
    (update-in event [:kixi.comms.event/payload :kixi.datastore.communication-specs/file-metadata-update-type]
               keyword)
    event))

(defn keyword-sharing-update-type
  [event]
  (if (= :kixi.datastore.communication-specs/file-metadata-sharing-updated
         (get-in event [:kixi.comms.event/payload :kixi.datastore.communication-specs/file-metadata-update-type]))
    (-> event
        (update-in [:kixi.comms.event/payload :kixi.datastore.metadatastore/sharing-update]
                   keyword)
        (update-in [:kixi.comms.event/payload :kixi.datastore.metadatastore/activity]
                   keyword))

    event))

(def missing-events (prep-events
                     (map keyword-sharing-update-type
                      (map keyword-update-type
                           (map #(update % :kixi.comms.event/key keyword)
                                (json/parse-stream (io/reader "./resources/missing-events.json") keyword))))))



(def first-file-events (sort-by :kixi.comms.event/created-at (butlast missing-events)))

(def second-file-events [(last missing-events)])


(def first-file-name "./event-log/new-format/prod-witan-event-delivery-1-2017-10-17-17-43-46-e32c5d2d-6174-423e-ab73-229c73d28e6c")

(def second-file-name "./event-log/new-format/prod-witan-event-delivery-1-2017-10-18-10-43-46-a33c1d8d-3174-523e-ab73-229c73d28e6c")


(defn write-file
  [file events]
  (with-open [out (io/output-stream file)]
    (doseq [event events]
      (as-> event e
        (update e :event nippy/freeze)
        (select-keys e
                     [:event
                      :sequence-num
                      :partition-key])
        (nippy/freeze e)
        ((baldr/baldr-writer out) ^bytes e)))))

;(write-file first-file-name first-file-events)
; (write-file second-file-name second-file-events)
