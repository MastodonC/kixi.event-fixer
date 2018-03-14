(ns kixi.event-fixer-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [kixi.event-backup-to-event-log.file-size :refer [correct-file-size]]
            [kixi.event-backup-to-event-log.old-format-parser :refer [file->events]]
            [kixi.event-backup-to-event-log :refer :all]))

(deftest prod-witan-event-1-2017-06-30-11-30-20-8fa8c413-ccf6-4c78-a7d4-d214ca33aa89
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-06-30-11-30-20-8fa8c413-ccf6-4c78-a7d4-d214ca33aa89")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))


(deftest prod-witan-event-1-2017-04-22-11-41-45-7263dc34-4496-46b7-9021-009137dfbb7f
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-04-22-11-41-45-7263dc34-4496-46b7-9021-009137dfbb7f")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))

(deftest prod-witan-event-1-2017-04-22-11-39-28-37c8ad21-091d-4852-bae4-b75c0f30abb4
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-04-22-11-39-28-37c8ad21-091d-4852-bae4-b75c0f30abb4")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))

(deftest prod-witan-event-1-2017-05-09-10-09-09-341b69f9-26ca-49e9-8967-0a8600592401
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-05-09-10-09-09-341b69f9-26ca-49e9-8967-0a8600592401")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))


(deftest prod-witan-event-1-2017-05-09-15-22-54-78390046-aa4c-47e4-81a3-2ce8308d58ee
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-05-09-15-22-54-78390046-aa4c-47e4-81a3-2ce8308d58ee")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))

(deftest prod-witan-event-1-2017-05-09-15-24-12-b55bcdf6-91c2-4747-85ff-2b91145465cc
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-05-09-15-24-12-b55bcdf6-91c2-4747-85ff-2b91145465cc")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))

(deftest prod-witan-event-1-2017-05-09-15-33-51-10d6eda9-51d1-4fd5-80fe-490b5cce7348
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-05-09-15-33-51-10d6eda9-51d1-4fd5-80fe-490b5cce7348")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))

(deftest prod-witan-event-1-2017-06-28-10-51-28-22578722-3c84-44da-b5c8-2aa8f3ee7d70
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-06-28-10-51-28-22578722-3c84-44da-b5c8-2aa8f3ee7d70")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))

(deftest prod-witan-event-1-2017-06-28-10-57-44-51daaf74-aeaa-4069-8e8d-cc99f866c8fb
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-06-28-10-57-44-51daaf74-aeaa-4069-8e8d-cc99f866c8fb")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))

(deftest prod-witan-event-1-2017-06-30-11-48-50-201ed935-d32f-4660-88b6-6e1d4a64bf7f
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-06-30-11-48-50-201ed935-d32f-4660-88b6-6e1d4a64bf7f")]
    (is (= []
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))

(deftest prod-witan-event-1-2017-07-11-09-03-30-800fbb87-bbae-4128-8c0b-780ae2c1d113
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-07-11-09-03-30-800fbb87-bbae-4128-8c0b-780ae2c1d113")]
    (is (empty?
           (transduce
            (comp file->events
                  (filter :error))
            conj
            [file])))))

(deftest file-size-byte-count-fixed
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-07-11-09-03-30-800fbb87-bbae-4128-8c0b-780ae2c1d113")
        file-created-events (into []
                                  (comp file->events
                                        (filter #(or (= :kixi.datastore.file/created
                                                        (get-in % [:event :kixi.comms.event/key]))
                                                     (= :kixi.datastore.communication-specs/file-metadata-created
                                                        (get-in % [:event :kixi.comms.event/payload :kixi.datastore.communication-specs/file-metadata-update-type]))))
                                        (map correct-file-size))
                                  [file])]
    (is (= 4
           (count file-created-events)))
    (is (empty?
         (remove number?
                 (map #(or (:kixi.datastore.metadatastore/size-bytes %)
                           ((comp :kixi.datastore.metadatastore/size-bytes :kixi.datastore.metadatastore/file-metadata) %))
                      (map (comp :kixi.comms.event/payload :event)
                           file-created-events)))))))

(deftest file-size-byte-count-fixed-2
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-07-20-10-27-33-5f20672e-d295-428f-8d4d-f12935d0bd0e")
        file-created-events (into []
                                  (comp file->events
                                        (filter #(or (= :kixi.datastore.file/created
                                                        (get-in % [:event :kixi.comms.event/key]))
                                                     (= :kixi.datastore.communication-specs/file-metadata-created
                                                        (get-in % [:event :kixi.comms.event/payload :kixi.datastore.communication-specs/file-metadata-update-type]))))
                                        (map correct-file-size))
                                  [file])]
    (is (= 2
           (count file-created-events)))
    (is (empty?
         (remove number?
                 (map #(or (:kixi.datastore.metadatastore/size-bytes %)
                            ((comp :kixi.datastore.metadatastore/size-bytes :kixi.datastore.metadatastore/file-metadata) %))
                      (map (comp :kixi.comms.event/payload :event)
                           file-created-events)))))))

(deftest read-md-count-doesnt-over-read
  (let [file (io/file "./event-log/old-format/prod-witan-event-1-2017-06-30-11-48-50-201ed935-d32f-4660-88b6-6e1d4a64bf7f")]
    (is (= ["https://prod-witan-kixi-datastore-filestore.s3-eu-west-1.amazonaws.com/6b251f93-60a9-40eb-8ce2-9ba1c05fed8c?X-Amz-Security-Token=FQoDYXdzEIz%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDCWuQidIAgwSSFPWNCK3A6Af48%2BuUaxOAdU8mU%2BD6n%2FcQHTADYNssf56MOQhaLnVdtJWfhCGiTQyUNl0KHKKPhbiRtWFx3Ni7pY%2FKaTGF0uNnyO4PPiuSJxxGJCYfEnhccSbaZtmyWcI%2FNcscvmdV%2FMRoAQfUOqXtV3KAs%2FrGAhbMllyKdvz%2FSunvpoRCK3Z1Z6%2BQpXobUIz5t3IMgPltm3fnQQGg7j%2B1ONnXqmQLIy1OL3pOCKk8wMmuqqpp81kTzexcsPnmQqRrYqv4ZFnUdJK3ri56sG5wJCBEFVC6CJ4s%2ByWglW8uE8BbN0G8RutVq1jmheOMdyttoT21JPTnIiflgjDj5ozFPmv%2B7jAEbPmmuT8EMQC3GKPbnKj25TF2fNYUlpolXwhCuuOlse6EStDaEAbqUvAboFMeAGJwv8241lQWumBqIfD0TEp7Cc8ZJilZgUBPjNEoL3fr0gB7GMZyTPnObhC2KjI3yTOP5%2FjMZrd9SjsEvhF7jtoMOL5gjMU%2FRmCrbd3XQozcL1KZi2uHj0HLJPj4r7CFf2j8hT9jX8fq4CdeogE7294zWcTYhQdIz75QSgWDhYqlNX8hRBENcug8A4otujYygU%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20170630T114917Z&X-Amz-SignedHeaders=host&X-Amz-Expires=1799&X-Amz-Credential=ASIAJOPAN4DWKASZPHQQ%2F20170630%2Feu-west-1%2Fs3%2Faws4_request&X-Amz-Signature=b3a900b70a71ddbd65a5bc2e5e103d885cf5d52c288454b389f48f1591fc2606"]
           (transduce
            (comp file->events
                 (map :event)
                 (filter #(= (:kixi.comms.event/id %) "06575196-0869-4eb4-a4ea-ab4f3eb81935"))
                 (map :kixi.comms.event/payload)
                 (map :kixi.datastore.filestore/upload-link))
            conj
            [file])))))
