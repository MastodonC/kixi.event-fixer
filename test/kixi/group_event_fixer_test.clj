(ns kixi.group-event-fixer-test
  (:require [kixi.group-event-fixer :as sut]
            [clojure.test :refer :all]))

(def user-event {:kixi.comms.message/type "event",
                 :kixi.comms.event/id "23a4b7cf-11a7-46b6-9c51-a5a7cacd4d74",
                 :kixi.comms.event/key :kixi.heimdall/group-created,
                 :kixi.comms.event/version "2.0.0",
                 :kixi.comms.event/created-at "20170630T112908.650Z",
                 :kixi.comms.event/payload
                 {:group-name "David Phillips",
                  :created "2017-06-07T16:19:43.624Z",
                  :group-type "user",
                  :id "d6650967-61e3-4145-bc0b-add0198ae4da",
                  :created-by "92ffa075-8767-4654-b0ff-06de9c0d9c57"},
                 :kixi.comms.event/origin "azrael"})

(deftest user-type-create-groups-discarded
  (is (= nil
         (sut/correct-event
          user-event))))

(def transform {:kixi.comms.message/type "event",
                :kixi.comms.event/id "746dc95e-a4b2-4619-8c8a-0539a96bbef5",
                :kixi.comms.event/key :kixi.heimdall/group-created,
                :kixi.comms.event/version "2.0.0",
                :kixi.comms.event/created-at "20170630T112908.586Z",
                :kixi.comms.event/payload
                {:group-name "Mastodon C_Energy Systems Catapult",
                 :created "2017-05-10T10:49:09.991Z",
                 :group-type "group",
                 :id "5b3225a1-db08-4930-aabe-c1972ab6832e",
                 :created-by "65661685-6757-45cf-8048-1ad6ecb6f8bb"},
                :kixi.comms.event/origin "azrael"})

(def transformed {:kixi.comms.message/type "event",
                  :kixi.comms.event/id "746dc95e-a4b2-4619-8c8a-0539a96bbef5",
                  :kixi.comms.event/key :kixi.heimdall/group-created,
                  :kixi.comms.event/version "2.0.0",
                  :kixi.comms.event/created-at "20170630T112908.586Z",
                  :kixi.comms.event/payload
                  {:group-name "Mastodon C_Energy Systems Catapult",
                   :created "2017-05-10T10:49:09.991Z",
                   :group-type "group",
                   :group-id "5b3225a1-db08-4930-aabe-c1972ab6832e",
                   :user-id "65661685-6757-45cf-8048-1ad6ecb6f8bb"},
                  :kixi.comms.event/origin "azrael"})

(deftest old-payload-transformed-to-new
  (is (= transformed
         (sut/correct-event
          transform))))

(def add-group-type {:kixi.comms.message/type "event",
                     :kixi.comms.event/id "8e00f6d0-e413-4960-b78c-0e45a279a966",
                     :kixi.comms.event/key :kixi.heimdall/group-created,
                     :kixi.comms.event/version "2.0.0",
                     :kixi.comms.event/created-at "20170815T095700.608Z",
                     :kixi.comms.event/payload
                     {:group-name "Mastodon C Adult Social Care",
                      :group-id "4f3f15ec-4cae-4c98-8b58-c9694691505d",
                      :created "2017-08-15T09:57:00.607Z",
                      :user-id "07169a2e-c2f0-4c95-a94e-5c46a614a5d6"},
                     :kixi.comms.event/origin "53955a3350d1"})

(def added-group-type {:kixi.comms.message/type "event",
                       :kixi.comms.event/id "8e00f6d0-e413-4960-b78c-0e45a279a966",
                       :kixi.comms.event/key :kixi.heimdall/group-created,
                       :kixi.comms.event/version "2.0.0",
                       :kixi.comms.event/created-at "20170815T095700.608Z",
                       :kixi.comms.event/payload
                       {:group-name "Mastodon C Adult Social Care",
                        :group-id "4f3f15ec-4cae-4c98-8b58-c9694691505d",
                        :created "2017-08-15T09:57:00.607Z",
                        :user-id "07169a2e-c2f0-4c95-a94e-5c46a614a5d6"
                        :group-type "group"},
                       :kixi.comms.event/origin "53955a3350d1"})

(deftest new-payloads-get-group-type
  (is (= added-group-type
         (sut/correct-event
          add-group-type))))

(deftest all-together
  (is (= [{:event transformed} {:event added-group-type}]
         (keep sut/correct-group-created-events
               [{:event user-event} {:event transform} {:event add-group-type}]))))
