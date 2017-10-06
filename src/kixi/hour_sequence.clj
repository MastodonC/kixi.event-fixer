(ns kixi.hour-sequence
  (:require [clj-time.core :as t]
            [clj-time.format :as f]
            [clj-time.periodic :as p]))

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
