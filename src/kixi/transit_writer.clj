(ns kixi.transit-writer
  (:require [clojure.java.io :as io]
            [cognitect.transit :as transit])
  (:import java.io.File))

(defn event->file-name
  [local-base-dir
   {:keys [^File file] :as event}]
  (apply str local-base-dir "/" (.getName file)))

(defn stacktrace-element->vec
  [^StackTraceElement ste]
  [(.getClassName ste) (.getFileName ste) (.getLineNumber ste) (.getMethodName ste)])

(defn exception->map
  [^Throwable e]
  (merge
   {:type (str (type e))
    :trace (apply str (mapv stacktrace-element->vec (.getStackTrace e)))}
   (when-let [m (.getMessage e)]
     {:message m})
   (when-let [c (.getCause e)]
     {:cause (exception->map c)})))

(def transit-handlers
  {java.util.regex.Pattern (transit/write-handler "pattern" (fn [o] (str o)))
   java.lang.Throwable (transit/write-handler "throwable" (fn [o] (exception->map o)))
   java.lang.Exception (transit/write-handler "exception" (fn [o] (exception->map o)))
   clojure.lang.ExceptionInfo (transit/write-handler "exceptioninfo" (fn [^clojure.lang.ExceptionInfo o]
                                                                       {:data (.getData o)
                                                                        :exception (exception->map o)}))})

(defn write-new-format
  [local-base-dir]
  (fn [event]
    (let [file (io/file (event->file-name local-base-dir event))]
      (when-not (.exists file)
        (.createNewFile file))
      (with-open [out (io/output-stream file :append true)]
        (if-not (:error event)
          (transit/write (transit/writer out
                                         :json
                                         {:handlers transit-handlers})
                         (select-keys event
                                      [:event
                                       :sequence-num
                                       :partition-key
                                       :dependencies]))
          (spit file (str event) :append true)))
      (spit file "\n" :append true))))
