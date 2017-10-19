(ns kixi.uploader-test
  (:require [kixi.uploader :as sut]
            [clojure.test :refer :all]
            [clojure.java.io :as io]))

(deftest determine-prefix-test
  (let [target (io/file sut/local-new-format-base-dir "prod-witan-event-delivery-1-2017-07-27-16-16-45-a9b1776e-ca53-435e-8ce5-d207166f6bdd")]
    (is (= ["2017/07/27/16" target]
           (sut/determine-prefix target)))))
