(ns kixi.event-searcher
  (:require   [clojure.java.io :as io]
              [baldr.core :as baldr]
              [taoensso.nippy :as nippy]))



(defn event-log->events
  "Produces a stream of all events from new format log directory"
  [log-dir]
  (sequence
   (comp (mapcat #(with-open [in (io/input-stream %)]
                    (doall (baldr/baldr-seq in))))
         (map nippy/thaw))
   (->> log-dir
        io/file
        file-seq
        rest)))

(defn filter-by-partition-key
  [pk]
  (filter #(= pk
              (:partition-key %))))

(def extract-event
  (comp (map :event)
        (map nippy/thaw)))

(def remarkable-two "f73e8fe0-079d-4d3f-8670-779cf53d4c00")

(def remarkable-three "7dbe7d1d-f893-49e5-868c-9962200c1506")

(def events
  (sort-by :kixi.comms.event/created-at
            (into []
                  (comp (filter-by-partition-key remarkable-three)
                        extract-event)
                  (event-log->events "./event-log/complete-new-log"))))

(count (event-log->events "./event-log/complete-new-log"))

events
