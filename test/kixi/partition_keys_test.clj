(ns kixi.partition-keys-test
  (:require [kixi.partition-keys :as pk]
            [kixi.event-fixer :refer [hour->s3-object-summaries
                                      object-summary->local-file]]
            [kixi.old-format-parser :refer [file->events]]
            [kixi.hour-sequence :refer [hour-sequence]]
            [clojure.test :as t]))

(def backup-start-hour "2017-06-28T12")

;(def backups-old-format-end-hour "2017-10-04T09")

(def backups-old-format-end-hour "2017-09-28T12")

(def staging-backup-s3-base "staging-witan-event-backup")
(def prod-backup-s3-base "prod-witan-event-backup")

(def local-old-format-base-dir "./event-log/old-format")
(def local-new-format-base-dir "./event-log/new-format")
