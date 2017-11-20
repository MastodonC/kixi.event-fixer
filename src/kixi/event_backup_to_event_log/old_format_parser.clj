(ns kixi.event-backup-to-event-log.old-format-parser
  (:require [clojure.java.io :as io]
            [gloss.io :as gloss.io]
            [kixi.event-backup-to-event-log.nippy :as nippy])
  (:import java.io.InputStream
           java.nio.ByteBuffer))

(def line-break 10)

(def decode-buffer-size (int 1024))

(defn input-stream->lines
  "Splits input stream into byte buffers by line-breaks or decode-buffer-size."
  ([[file ^InputStream rdr]]
   (input-stream->lines [file rdr] (ByteBuffer/allocate decode-buffer-size)))
  ([[file ^InputStream rdr]
    ^ByteBuffer buffer]
   (if-not (.hasRemaining buffer)
     (cons buffer
           (lazy-seq (input-stream->lines rdr)))
     (loop [b (.read rdr)]
       (when-not (= -1 b)
         (.put buffer (unchecked-byte b))
         (if (or (= line-break b)
                 (not (.hasRemaining buffer)))
           (cons [file buffer]
                 (lazy-seq (input-stream->lines [file rdr])))
           (recur (.read rdr))))))))

(def nippy-head-sig
  "First 3 bytes of Nippy header"
  (.getBytes "NPY" "UTF-8"))

(def nippy-version-1 (byte 0))

(defn npy-headed-buffer?
  "True if first bytes in buffer match sig"
  [^ByteBuffer buffer]
  (let [^bytes nhs nippy-head-sig]
    (and (= (aget nhs 0)
            (.get buffer 0))
         (= (aget nhs 1)
            (.get buffer 1))
         (= (aget nhs 2)
            (.get buffer 2))
         (= nippy-version-1
            (.get buffer 3)))))

(defn ^ByteBuffer rewind-buffer
  [^ByteBuffer b]
  (.rewind b))

(defn trim-to-data
  "Creates a new buffer limited to that portion of the parent that contains data"
  [[file ^ByteBuffer buffer]]
  (let [position (.position buffer)]
    [file (-> buffer (rewind-buffer) (.slice) (.limit position))]))

(defn partition-into-nippy-sequence
  "Partitions seq into vectors containing all parts of an event"
  [xf]
  (let [a (java.util.ArrayList.)]
    (fn
      ([] (xf))
      ([acc]
       (let [complete-seq (vec (.toArray a))]
         (.clear a)
         (->> [(ffirst complete-seq) (map second complete-seq)]
              (xf acc)
              xf)))
      ([acc [file buffer]]
       (if (or (.isEmpty a)
               (not (npy-headed-buffer? buffer)))
         (do
           (.add a [file buffer])
           acc)
         (let [complete-seq (vec (.toArray a))]
           (.clear a)
           (.add a [file buffer])
           (xf acc [(ffirst complete-seq) (map second complete-seq)])))))))

(defn combine-nippy-sequence
  [[file nippy-seq]]
  [file (gloss.io/contiguous (map rewind-buffer nippy-seq))])

(defn drop-last-byte
  [[file ^ByteBuffer buffer]]
  (let [size (dec (.capacity buffer))]
    [file (ByteBuffer/wrap (byte-array size (butlast (seq (.array buffer)))))]))

(defn buffer->event
  [[num ^java.io.File file ^java.nio.ByteBuffer barray]]
  (try
    (when-let [event (nippy/thaw (.array barray))]
      {:event event
       :event-str (String. (.array barray))
       :file file
       :event-counter num})
    (catch Exception e
      (try
        (prn (.getName file) "-" e)
        {:event-str (String. (.array barray))
         :file file
         :error true
         :event-counter num
         :exception (str (.getClass e))}
        (catch Exception e
          {:file file
           :error true
           :event-counter num
           :exception (str (.getClass e))})))))

(defn event->event-plus-event-counter
  [xf]
  (let [sequence-num-seq (atom (range (Long/MAX_VALUE)))]
    (fn
      ([] (xf))
      ([acc] (xf acc))
      ([acc event]
       (let [num (first @sequence-num-seq)]
         (swap! sequence-num-seq rest)
         (xf acc (cons num event)))))))

(def file->events
  (comp
   (map #(vector % (io/input-stream %)))
   (mapcat input-stream->lines)
   (map trim-to-data)
   partition-into-nippy-sequence
   (map combine-nippy-sequence)
   (map drop-last-byte)
   event->event-plus-event-counter
   (keep buffer->event)))
