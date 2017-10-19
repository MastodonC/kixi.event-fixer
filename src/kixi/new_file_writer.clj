(ns kixi.new-file-writer
  (:require [baldr.core :as baldr]
            [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [clojure.string :as string])
  (:import [java.io File]))

(defn event->file-name
  [local-base-dir
   {:keys [^File file] :as event}]
  (apply str local-base-dir "/" (string/replace (.getName file) "witan-event" "witan-event-delivery")))


(defn write-new-format
  [local-base-dir]
  (fn [event]
    (let [file (io/file (event->file-name local-base-dir event))]
      (when-not (.exists file)
        (.createNewFile file))
      (if-not (:error event)
        (with-open [out (io/output-stream file :append true)]
          (as-> event e
            (update e :event nippy/freeze)
            (select-keys e
                         [:event
                          :sequence-num
                          :partition-key])
            (nippy/freeze e)
            ((baldr/baldr-writer out) ^bytes e)))
        (do (prn "Writing Error")
            (spit file (str event) :append true))))))
