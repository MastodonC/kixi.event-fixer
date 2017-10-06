(ns kixi.transit-writer
  (:require [clojure.java.io :as io]
            [cognitect.transit :as transit])
  (:import java.io.File))

(defn event->file-name
  [local-base-dir
   {:keys [^File file] :as event}]
  (apply str local-base-dir "/" (.getName file)))

(defn write-new-format
  [local-base-dir]
  (fn [event]
    (let [file (io/file (event->file-name local-base-dir event))]
      (when-not (.exists file)
        (.createNewFile file))
      (with-open [out (io/output-stream file :append true)]
        (if-not (:error event)
          (transit/write (transit/writer out :json)
                         (select-keys event
                                      [:event
                                       :sequence-num
                                       :partition-key
                                       :dependencies]))
          (spit file (str event) :append true)))
      (spit file "\n" :append true))))
