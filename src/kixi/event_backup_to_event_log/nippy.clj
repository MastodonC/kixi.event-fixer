(ns kixi.event-backup-to-event-log.nippy
  "High-performance serialization library for Clojure"
  {:author "Peter Taoussanis (@ptaoussanis)"}
  (:require [clojure.java.io :as jio]
            [kixi.event-backup-to-event-log.nippy.compression :as compression]
            [kixi.event-backup-to-event-log.nippy.encryption :as encryption]
            [kixi.event-backup-to-event-log.nippy.utils :as utils]
            [taoensso.encore :as enc :refer [cond*]])
  (:import [clojure.lang APersistentMap APersistentSet APersistentVector BigInt IPersistentMap IRecord ISeq Keyword LazySeq PersistentList PersistentQueue PersistentTreeMap PersistentTreeSet Ratio Symbol]
           [java.io ByteArrayInputStream ByteArrayOutputStream DataInput DataInputStream DataOutput DataOutputStream ObjectInputStream ObjectOutputStream Serializable]
           java.lang.reflect.Method
           [java.util Date UUID]
           java.util.regex.Pattern))

(if (vector? enc/encore-version)
  (enc/assert-min-encore-version [2 67 1])
  (enc/assert-min-encore-version  2.67))

(do
  (enc/defalias compress          compression/compress)
  (enc/defalias decompress        compression/decompress)
  (enc/defalias snappy-compressor compression/snappy-compressor)
  (enc/defalias lzma2-compressor  compression/lzma2-compressor)
  (enc/defalias lz4-compressor    compression/lz4-compressor)
  (enc/defalias lz4hc-compressor  compression/lz4hc-compressor)

  (enc/defalias encrypt           encryption/encrypt)
  (enc/defalias decrypt           encryption/decrypt)
  (enc/defalias aes128-encryptor  encryption/aes128-encryptor)

  (enc/defalias freezable?        utils/freezable?))

(comment
  (set! *unchecked-math* :warn-on-boxed)
  (set! *unchecked-math* false)
  (thaw (freeze stress-data)))

;;;; TODO
;; - Performance would benefit from ^:static support / direct linking / etc.
;; - Ability to compile out metadata support?
;; - Auto cache keywords? When map keys? Configurable? Per-map
;;   (`cache-keys`)? Just rely on compression?

;;;; Nippy data format
;; * 4-byte header (Nippy v2.x+) (may be disabled but incl. by default) [1]
;; { * 1-byte type id
;;   * Arb-length payload determined by freezer for this type [2] } ...
;;
;; [1] Inclusion of header is *strongly* recommended. Purpose:
;;   * Sanity check (confirm that data appears to be Nippy data)
;;   * Nippy version check (=> supports changes to data schema over time)
;;   * Supports :auto thaw compressor, encryptor
;;   * Supports :auto freeze compressor (since this depends on :auto thaw
;;     compressor)
;;
;; [2] See `IFreezable1` protocol for type-specific payload formats,
;;     `thaw-from-in!` for reference type-specific thaw implementations
;;
(def ^:private ^:const charset "UTF-8")
(def ^:private head-sig "First 3 bytes of Nippy header" (.getBytes "NPY" charset))
(def ^:private ^:const head-version "Current Nippy header format version" 1)
(def ^:private ^:const head-meta
  "Final byte of 4-byte Nippy header stores version-dependent metadata"
  {(byte 0)  {:version 1 :compressor-id nil     :encryptor-id nil}
   (byte 4)  {:version 1 :compressor-id nil     :encryptor-id :else}
   (byte 5)  {:version 1 :compressor-id :else   :encryptor-id nil}
   (byte 6)  {:version 1 :compressor-id :else   :encryptor-id :else}
   ;;
   (byte 2)  {:version 1 :compressor-id nil     :encryptor-id :aes128-sha512}
   ;;
   (byte 1)  {:version 1 :compressor-id :snappy :encryptor-id nil}
   (byte 3)  {:version 1 :compressor-id :snappy :encryptor-id :aes128-sha512}
   (byte 7)  {:version 1 :compressor-id :snappy :encryptor-id :else}
   ;;
   ;;; :lz4 used for both lz4 and lz4hc compressor (the two are compatible)
   (byte 8)  {:version 1 :compressor-id :lz4    :encryptor-id nil}
   (byte 9)  {:version 1 :compressor-id :lz4    :encryptor-id :aes128-sha512}
   (byte 10) {:version 1 :compressor-id :lz4    :encryptor-id :else}
   ;;
   (byte 11) {:version 1 :compressor-id :lzma2  :encryptor-id nil}
   (byte 12) {:version 1 :compressor-id :lzma2  :encryptor-id :aes128-sha512}
   (byte 13) {:version 1 :compressor-id :lzma2  :encryptor-id :else}})

(defmacro ^:private when-debug [& body] (when #_true false `(do ~@body)))

(def ^:private type-ids
  "{<byte-id> <type-name-kw>}, ~random ordinal ids for historical reasons.
  -ive ids reserved for custom (user-defined) types.

  Size-optimized suffixes:
    -0  (empty       => 0-sized)
    -sm (small       => byte-sized)
    -md (medium      => short-sized)
    -lg (large       => int-sized)   ; Default when no suffix
    -xl (extra large => long-sized)"

  {82  :prefixed-custom

   47  :reader-sm
   51  :reader-md
   52  :reader-lg
   5   :reader-lg2 ; == :reader-lg, used only for back-compatible thawing

   46  :serializable-sm
   50  :serializable-md
   6   :serializable-lg ; Used only for back-compatible thawing

   48  :record-sm
   49  :record-md
   80  :record-lg ; Used only for back-compatible thawing

   81  :type ; TODO Implement?

   3   :nil
   8   :true
   9   :false
   10  :char

   34  :str-0
   105 :str-sm
   16  :str-md
   13  :str-lg

   106 :kw-sm
   14  :kw-lg

   56  :sym-sm
   57  :sym-lg

   58  :regex
   71  :uri ; TODO Implement?

   53  :bytes-0
   7   :bytes-sm
   15  :bytes-md
   2   :bytes-lg

   17  :vec-0
   113 :vec-2
   114 :vec-3
   110 :vec-sm
   69  :vec-md
   21  :vec-lg

   18  :set-0
   111 :set-sm
   32  :set-md
   23  :set-lg

   19  :map-0
   112 :map-sm
   33  :map-md
   30  :map-lg

   35  :list-0
   36  :list-sm
   54  :list-md
   20  :list-lg

   37  :seq-0
   38  :seq-sm
   39  :seq-md
   24  :seq-lg

   28  :sorted-set
   31  :sorted-map
   26  :queue
   25  :meta

   40  :byte
   41  :short
   42  :integer

   0   :long-zero
   100 :long-sm
   101 :long-md
   102 :long-lg
   43  :long-xl

   44  :bigint
   45  :biginteger

   60  :float
   55  :double-zero
   61  :double
   62  :bigdec
   70  :ratio

   90  :date
   91  :uuid

   59  :cached-0
   63  :cached-1
   64  :cached-2
   65  :cached-3
   66  :cached-4
   72  :cached-5
   73  :cached-6
   74  :cached-7
   67  :cached-sm
   68  :cached-md

   ;;; DEPRECATED (old types are supported only for thawing)
   1   :reader-depr1       ; v0.9.2+ for +64k support
   11  :str-depr1          ; v0.9.2+ for +64k support
   22  :map-depr1          ; v0.9.0+ for more efficient thaw
   12  :kw-depr1           ; v2.0.0-alpha5+ for str consistecy
   27  :map-depr2          ; v2.11+ for count/2
   29  :sorted-map-depr1   ; v2.11+ for count/2
   4   :boolean-depr1      ; v2.12+ for switch to true/false ids
   })

(comment
  (defn- get-free-byte-ids [ids-map]
    (reduce (fn [acc in] (if-not (ids-map in) (conj acc in) acc))
      [] (range 0 Byte/MAX_VALUE)))

  (count (get-free-byte-ids type-ids)))

(defmacro ^:private defids []
  `(do
     ~@(map
         (fn [[id# name#]]
           (let [name# (str "id-" (name name#))
                 sym#  (with-meta (symbol name#)
                         {:const true :private true})]
             `(def ~sym# (byte ~id#))))
         type-ids)))

(comment (macroexpand '(defids)))

(defids)


;;;; Dynamic config
;; See also `nippy.tools` ns for further dynamic config support

;; TODO Switch to thread-local proxies?

(enc/defonce ^:dynamic *freeze-fallback* "(fn [data-output x]), nil => default" nil)
(enc/defonce ^:dynamic *custom-readers* "{<hash-or-byte-id> (fn [data-input])}" nil)
(enc/defonce ^:dynamic *auto-freeze-compressor*
  "(fn [byte-array])->compressor used by `(freeze <x> {:compressor :auto}),
  nil => default"
  nil)

(defn set-freeze-fallback!        [x] (alter-var-root #'*freeze-fallback*        (constantly x)))
(defn set-auto-freeze-compressor! [x] (alter-var-root #'*auto-freeze-compressor* (constantly x)))
(defn swap-custom-readers!        [f] (alter-var-root #'*custom-readers* f))

(declare ^:dynamic *final-freeze-fallback*) ; DEPRECATED

;;;; Freezing

#_(do
  (defmacro write-id [out id] `(.writeByte ~out ~id))

  (defmacro ^:private sm-count? [n] `(<= ~n 255))   #_(- Byte/MAX_VALUE  Byte/MIN_VALUE)
  (defmacro ^:private md-count? [n] `(<= ~n 65535)) #_(- Short/MAX_VALUE Short/MIN_VALUE)

  (defmacro ^:private write-sm-count [out n]
    `(if (<= ~n 127)
       (.writeByte ~out ~n)
       (.writeByte ~out (unchecked-subtract 127 ~n))))

  (defmacro ^:private write-md-count [out n]
    `(if (<= ~n 32767)
       (.writeShort ~out ~n)
       (.writeShort ~out (unchecked-subtract 32767 ~n))))

  (defmacro ^:private write-lg-count [out n] `(.writeInt ~out ~n))

  (defmacro ^:private read-sm-count [in]
    `(let [n# (.readByte ~in)]
       (if (pos? n#)
         n#
         (unchecked-subtract 127 n#))))

  (defmacro ^:private read-md-count [in]
    `(let [n# (.readShort ~in)]
       (if (pos? n#)
         n#
         (unchecked-subtract 32767 n#))))

  (defmacro ^:private read-lg-count [in] `(.readInt ~in)))



(do
  (defmacro write-id [out id] `(.writeByte ~out ~id))

  (defmacro ^:private sm-count? [n] `(<= ~n 127))
  (defmacro ^:private md-count? [n] `(<= ~n 32767))

  (defmacro ^:private write-sm-count [out n] `(.writeByte  ~out ~n))
  (defmacro ^:private write-md-count [out n] `(.writeShort ~out ~n))
  (defmacro ^:private write-lg-count [out n] `(.writeInt   ~out ~n))



  (defmacro ^:private read-lg-count [in] `(.readInt   ~in)))

(defn read-sm-count
  [^DataInputStream in]
  (.readByte in))

(def kw-sm 106)
(def kw-lg 14)

(def buffer-size 40000)

(defn raw-count-to-nxt-kw
  [^DataInputStream in]
  (:marker-start
   (reduce
    (fn [state read-count]
      (let [nxt (.read in)]
        (case (:marker-state state)
          :none
          (if (or (= kw-sm nxt)
                  (= kw-lg nxt))
            {:marker-state :kw
             :marker-start read-count}
            state)
          :kw
          (assoc state :marker-state :skipped-count-byte)
          :skipped-count-byte
          (if (= (int \k) nxt)
            (assoc state :marker-state :k)
            {:marker-state :none})
          :k
          (if (= (int \i) nxt)
            (assoc state :marker-state :i)
            {:marker-state :none})
          :i
          (if (= (int \x) nxt)
            (assoc state :marker-state :x)
            {:marker-state :none})
          :x
          (if (= (int \i) nxt)
            (reduced (assoc state :marker-state :done))
            {:marker-state :none}))))
    {:marker-state :none}
    (range buffer-size))))

(defn byte-count-to-nxt-kw
  [^DataInputStream in]
  (.mark in buffer-size)
  (let [next-marker-location (raw-count-to-nxt-kw in)]
    (.reset in)
    next-marker-location))

(defn skip-utf-8-encoded-chars
  [^DataInputStream in]
  (let [nxt (.read in)
        char-size-kw (cond
                       (and (bit-test nxt 7)
                            (bit-test nxt 6)
                            (bit-test nxt 5)
                            (bit-test nxt 4)
                            (not (bit-test nxt 3))) :four-byte-char
                       (and (bit-test nxt 7)
                            (bit-test nxt 6)
                            (bit-test nxt 5)
                            (not (bit-test nxt 4))) :three-byte-char
                       (and (bit-test nxt 7)
                            (bit-test nxt 6)
                            (not (bit-test nxt 5))) :two-byte-char
                       :default :one-byte-char)]
    (case char-size-kw
      :four-byte-char (do (.read in) (.read in) (.read in))
      :three-byte-char (do (.read in) (.read in))
      :two-byte-char (.read in)
      :one-byte-char :already-read)))

(defn ^:private read-md-count
  "Supposed to be a short, two bytes, but both might have been corrupted into multi byte utf-8 chars."
  [^DataInputStream in]

  (skip-utf-8-encoded-chars in)
  (skip-utf-8-encoded-chars in)

  (byte-count-to-nxt-kw in))

 ; We extend `IFreezable1` to supported types:
(defprotocol     IFreezable1 (-freeze-without-meta! [x data-output]))
(defprotocol     IFreezable2 (-freeze-with-meta!    [x data-output]))
(extend-protocol IFreezable2 ; Must be a separate protocol
  clojure.lang.IMeta
  (-freeze-with-meta! [x ^DataOutput data-output]
    (let [m (.meta x)]
      (when m
        (write-id data-output id-meta)
        (-freeze-without-meta! m data-output)))
    (-freeze-without-meta!     x data-output))

  nil
  (-freeze-with-meta! [x data-output]
    (-freeze-without-meta! x data-output))

  Object
  (-freeze-with-meta! [x data-output]
    (-freeze-without-meta! x data-output)))

(defn- write-bytes-sm [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    ;; (byte len)
    (write-sm-count out len)
    (.write         out ba 0 len)))

(defn- write-bytes-md [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    ;; (short len)
    (write-md-count out len)
    (.write         out ba 0 len)))

(defn- write-bytes-lg [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    (write-lg-count out len)
    (.write         out ba 0 len)))

(defn- write-bytes [^DataOutput out ^bytes ba]
  (let [len (alength ba)]
    (if (zero? len)
      (write-id out id-bytes-0)
      (do
        (cond*
          (sm-count? len)
          (do (write-id       out id-bytes-sm)
              (write-sm-count out len))

          (md-count? len)
          (do (write-id       out id-bytes-md)
              (write-md-count out len))

          :else
          (do (write-id       out id-bytes-lg)
              (write-lg-count out len)))

        (.write out ba 0 len)))))


(declare thaw-from-in!)

;;;; Thawing

(defn- read-bytes [^DataInput in len]
  (let [ba (byte-array len)]
    (.readFully in ba 0 len)
    ba))

(defn- read-bytes-sm [^DataInput in] (read-bytes in (read-sm-count in)))
(defn- read-bytes-md [^DataInput in] (read-bytes in (read-md-count in)))
(defn- read-bytes-lg [^DataInput in] (read-bytes in (read-lg-count in)))

(defn- read-utf8    [in len]        (String. ^bytes (read-bytes in len)                charset))
(defn- read-utf8-sm [^DataInput in] (String. ^bytes (read-bytes in (read-sm-count in)) charset))
(defn- read-utf8-md [^DataInput in] (String. ^bytes (read-bytes in (read-md-count in)) charset))
(defn- read-utf8-lg [^DataInput in] (String. ^bytes (read-bytes in (read-lg-count in)) charset))

(defn- read-biginteger [^DataInput in] (BigInteger. ^bytes (read-bytes in (.readInt in))))

(defmacro ^:private editable? [coll] `(instance? clojure.lang.IEditableCollection ~coll))

(defn- read-into [to ^DataInput in ^long n]
  (if (and (editable? to) (> n 10))
    (persistent!
      (enc/reduce-n (fn [acc _] (conj! acc (thaw-from-in! in)))
        (transient to) n))

    (enc/reduce-n (fn [acc _] (conj acc (thaw-from-in! in))) to n)))

(def long-zero 0)
(def long-sm 100)
(def long-md 101)
(def long-lg 102)

(defn byte-size-count
  [^DataInputStream in]
  (.mark in buffer-size)
  (let [nxt (.read in)
        correct-byte-count (condp = nxt
                             long-zero 0
                             long-sm 1
                             long-md 2
                             long-lg 4)
        _ (.skip in correct-byte-count)
        to-next-kw (raw-count-to-nxt-kw in)]
    (.reset in)
    (+ correct-byte-count
       to-next-kw)))

(defn assoc-thaw
  [^DataInputStream in assocer]
  (fn [acc _]
    (let [k (thaw-from-in! in)]
      (cond
        (= k :kixi.datastore.metadatastore/size-bytes)
        (do
          (let [skip-count (inc (byte-size-count in))]
            (.skip ^DataInputStream in (long skip-count))
            (assocer acc k :FIX-BYTE-COUNT)))

        (and (= :aws-rejected (:reason acc))
             (= k :explain))
        (do
          (let [skip-count (byte-count-to-nxt-kw in)]
            (.skip ^DataInputStream in (long skip-count))
            (assocer acc k :exception-removed)))

        :default (do (let [v (thaw-from-in! in)]
                       (assocer acc k v)))))))

(defn- read-kvs-into [to ^DataInputStream in ^long n]
  (if (and (editable? to) (> n 10))
    (persistent!
     (enc/reduce-n
      (assoc-thaw in assoc!)
      (transient to) n))

    (enc/reduce-n
     (assoc-thaw in assoc)
     to n)))

(defn- read-kvs-depr1 [to ^DataInput in] (read-kvs-into to in (quot (.readInt in) 2)))

(def ^:private class-method-sig (into-array Class [IPersistentMap]))

(defn- read-custom! [in prefixed? type-id]
  (if-let [custom-reader (get *custom-readers* type-id)]
    (try
      (custom-reader in)
      (catch Exception e
        (throw
          (ex-info
            (str "Reader exception for custom type id: " type-id)
            {:type-id type-id
             :prefixed? prefixed?} e))))
    (throw
      (ex-info
        (str "No reader provided for custom type id: " type-id)
        {:type-id type-id
         :prefixed? prefixed?}))))

(defn- read-edn [edn]
  (try
    (enc/read-edn {:readers *data-readers*} edn)
    (catch Exception e
      {:type :reader
       :throwable e
       :nippy/unthawable edn})))

(defn- read-serializable [^DataInput in class-name]
  (try
    (let [content (.readObject (ObjectInputStream. in))]
      (try
        (let [class (Class/forName class-name)] (cast class content))
        (catch Exception e
          {:type :serializable
           :throwable e
           :nippy/unthawable {:class-name class-name :content content}})))
    (catch Exception e
      {:type :serializable
       :throwable e
       :nippy/unthawable {:class-name class-name :content nil}})))

(defn- read-record [in class-name]
  (let [content (thaw-from-in! in)]
    (try
      (let [class  (clojure.lang.RT/classForName class-name)
            method (.getMethod class "create" class-method-sig)]
        (.invoke method class (into-array Object [content])))
      (catch Exception e
        {:type :record
         :throwable e
         :nippy/unthawable {:class-name class-name :content content}}))))

(declare thaw-from-in1)

(defn prn-t
  [x]
  (prn x)
  x)

(defn thaw-from-in!
  "Deserializes a frozen object from given DataInput to its original Clojure
  data type.

  This is a low-level util: in most cases you'll want `thaw` instead."
  [^DataInput data-input]
  (let [in      data-input
        type-id (.readByte in)]
    (try
      (enc/case-eval type-id

                     id-reader-sm       (read-edn             (read-utf8 in (read-sm-count in)))
                     id-reader-md       (read-edn             (read-utf8 in (read-md-count in)))
                     id-reader-lg       (read-edn             (read-utf8 in (read-lg-count in)))
                     id-reader-lg2      (read-edn             (read-utf8 in (read-lg-count in)))
                     id-serializable-sm (read-serializable in (read-utf8 in (read-sm-count in)))
                     id-serializable-md (read-serializable in (read-utf8 in (read-md-count in)))
                     id-serializable-lg (read-serializable in (read-utf8 in (read-lg-count in)))
                     id-record-sm       (read-record       in (read-utf8 in (read-sm-count in)))
                     id-record-md       (read-record       in (read-utf8 in (read-md-count in)))
                     id-record-lg       (read-record       in (read-utf8 in (read-lg-count in)))

                     id-nil         nil
                     id-true        true
                     id-false       false
                     id-char        (.readChar in)
                     id-meta        (let [m (thaw-from-in! in)]
                                      (with-meta (thaw-from-in! in) m))



                     id-bytes-0     (byte-array 0)
                     id-bytes-sm    (read-bytes in (read-sm-count in))
                     id-bytes-md    (read-bytes in (read-md-count in))
                     id-bytes-lg    (read-bytes in (read-lg-count in))

                     id-str-0       ""
                     id-str-sm               (read-utf8 in (read-sm-count in))
                     id-str-md               (read-utf8 in (read-md-count in))
                     id-str-lg               (read-utf8 in (read-lg-count in))
                     id-kw-sm       (keyword (read-utf8 in (read-sm-count in)))
                     id-kw-lg       (keyword (read-utf8 in (read-lg-count in)))
                     id-sym-sm      (symbol  (read-utf8 in (read-sm-count in)))
                     id-sym-lg      (symbol  (read-utf8 in (read-lg-count in)))
                     id-regex       (re-pattern (thaw-from-in! in))

                     id-vec-0       []
                     id-vec-2       [(thaw-from-in! in) (thaw-from-in! in)]
                     id-vec-3       [(thaw-from-in! in) (thaw-from-in! in) (thaw-from-in! in)]
                     id-vec-sm      (read-into [] in (read-sm-count in))
                     id-vec-md      (read-into [] in (read-md-count in))
                     id-vec-lg      (read-into [] in (read-lg-count in))

                     id-set-0       #{}
                     id-set-sm      (read-into    #{} in (read-sm-count in))
                     id-set-md      (read-into    #{} in (read-md-count in))
                     id-set-lg      (read-into    #{} in (read-lg-count in))

                     id-map-0       {}
                     id-map-sm      (read-kvs-into {} in (read-sm-count in))
                     id-map-md      (read-kvs-into {} in (read-md-count in))
                     id-map-lg      (read-kvs-into {} in (read-lg-count in))

                     id-queue       (read-into (PersistentQueue/EMPTY) in (read-lg-count in))
                     id-sorted-set  (read-into     (sorted-set)        in (read-lg-count in))
                     id-sorted-map  (read-kvs-into (sorted-map)        in (read-lg-count in))

                     id-list-0      '()
                     id-list-sm     (into '() (rseq (read-into [] in (read-sm-count in))))
                     id-list-md     (into '() (rseq (read-into [] in (read-md-count in))))
                     id-list-lg     (into '() (rseq (read-into [] in (read-lg-count in))))

                     id-seq-0       (lazy-seq nil)
                     id-seq-sm      (or (seq (read-into [] in (read-sm-count in))) (lazy-seq nil))
                     id-seq-md      (or (seq (read-into [] in (read-md-count in))) (lazy-seq nil))
                     id-seq-lg      (or (seq (read-into [] in (read-lg-count in))) (lazy-seq nil))

                     id-byte              (.readByte  in)
                     id-short             (.readShort in)
                     id-integer           (.readInt   in)
                     id-long-zero   0
                     id-long-sm     (long (.readByte  in))
                     id-long-md     (long (.readShort in))
                     id-long-lg     (long (.readInt   in))
                     id-long-xl           (.readLong  in)

                     id-bigint      (bigint (read-biginteger in))
                     id-biginteger          (read-biginteger in)

                     id-float       (.readFloat  in)
                     id-double-zero 0.0
                     id-double      (.readDouble in)
                     id-bigdec      (BigDecimal. ^BigInteger (read-biginteger in) (.readInt in))

                     id-ratio       (clojure.lang.Ratio.
                                     (read-biginteger in)
                                     (read-biginteger in))

                     id-date        (Date. (.readLong in))
                     id-uuid        (UUID. (.readLong in) (.readLong in))

                     ;; Deprecated ------------------------------------------------------
                     id-boolean-depr1    (.readBoolean in)
                     id-sorted-map-depr1 (read-kvs-depr1 (sorted-map) in)
                     id-map-depr2        (read-kvs-depr1 {} in)
                     id-reader-depr1     (read-edn (.readUTF in))
                     id-str-depr1                  (.readUTF in)
                     id-kw-depr1         (keyword  (.readUTF in))
                     id-map-depr1        (apply hash-map
                                                (enc/repeatedly-into [] (* 2 (.readInt in))
                                                                     (fn [] (thaw-from-in! in))))
                     ;; -----------------------------------------------------------------

                     id-prefixed-custom (read-custom! in :prefixed (.readShort in))

                     (if (neg? type-id)
                       (read-custom! in nil type-id) ; Unprefixed custom type
                       (throw
                        (ex-info
                         (str "Unrecognized type id (" type-id "). Data frozen with newer Nippy version?")
                         {:type-id type-id}))))

      (catch Exception e
        (throw (ex-info (str "Thaw failed against type-id: " type-id)
                        {:type-id type-id} e))))))

(let [head-sig head-sig] ; Not ^:const
  (defn- try-parse-header [^bytes ba]
    (let [len (alength ba)]
      (when (> len 4)
        (let [-head-sig (java.util.Arrays/copyOf ba 3)]
          (when (java.util.Arrays/equals -head-sig ^bytes head-sig)
            ;; Header appears to be well-formed
            (let [meta-id (aget ba 3)
                  data-ba (java.util.Arrays/copyOfRange ba 4 len)]
              [data-ba (get head-meta meta-id {:unrecognized-meta? true})])))))))

(defn read-data-input
  [^DataInputStream in]
  (.read in))

(defn thaw-delimited-maps-from-in
  ([data-input]
   (thaw-delimited-maps-from-in data-input 10))
  ([^DataInput data-input delimiter]
   ;discard header, obviously should check it
   (let [header-1 (read-data-input data-input)]
     (if (= -1 header-1)
       nil
       (do
         (read-data-input data-input)
         (read-data-input data-input)
         (read-data-input data-input)

         (cons (try
                 (thaw-from-in! data-input)
                 (catch Throwable e
                   (prn e)
                   :error-decoding))
               (lazy-seq (let [next-int (read-data-input data-input)]
                           (cond
                             (= -1 next-int) nil
                             (not= next-int delimiter) (throw (new Exception (str "Delimiter expected " delimiter " encountered " next-int)))
                             :default (thaw-delimited-maps-from-in data-input delimiter))))))))))

(defn- get-auto-compressor [compressor-id]
  (case compressor-id
    nil        nil
    :snappy    snappy-compressor
    :lzma2     lzma2-compressor
    :lz4       lz4-compressor
    :no-header (throw (ex-info ":auto not supported on headerless data." {}))
    :else (throw (ex-info ":auto not supported for non-standard compressors." {}))
    (throw (ex-info (str "Unrecognized :auto compressor id: " compressor-id)
             {:compressor-id compressor-id}))))

(defn- get-auto-encryptor [encryptor-id]
  (case encryptor-id
    nil            nil
    :aes128-sha512 aes128-encryptor
    :no-header     (throw (ex-info ":auto not supported on headerless data." {}))
    :else (throw (ex-info ":auto not supported for non-standard encryptors." {}))
    (throw (ex-info (str "Unrecognized :auto encryptor id: " encryptor-id)
             {:encryptor-id encryptor-id}))))

(def ^:private err-msg-unknown-thaw-failure
  "Decryption/decompression failure, or data unfrozen/damaged.")

(def ^:private err-msg-unrecognized-header
  "Unrecognized (but apparently well-formed) header. Data frozen with newer Nippy version?")

(defn thaw
  "Deserializes a frozen Nippy byte array to its original Clojure data type.
  To thaw custom types, extend the Clojure reader or see `extend-thaw`.

  ** By default, supports data frozen with Nippy v2+ ONLY **
  Add `{:v1-compatibility? true}` option to support thawing of data frozen with
  legacy versions of Nippy.

  Options include:
    :v1-compatibility? - support data frozen by legacy versions of Nippy?
    :compressor - :auto (checks header, default)  an ICompressor, or nil
    :encryptor  - :auto (checks header, default), an IEncryptor,  or nil"

  ([ba] (thaw ba nil))
  ([^bytes ba
    {:keys [v1-compatibility? compressor encryptor password]
     :or   {compressor :auto
            encryptor  :auto}
     :as   opts}]

   (assert (not (get opts :headerless-meta))
     ":headerless-meta `thaw` opt removed in Nippy v2.7+")

   (let [v2+?       (not v1-compatibility?)
         no-header? (get opts :no-header?) ; Intentionally undocumented
         ex (fn ex
              ([  msg] (ex nil msg))
              ([e msg] (throw (ex-info (str "Thaw failed: " msg)
                                {:opts (assoc opts
                                         :compressor compressor
                                         :encryptor  encryptor)}
                                e))))

         thaw-data
         (fn [data-ba compressor-id encryptor-id ex-fn]
           (let [compressor (if (identical? compressor :auto)
                              (get-auto-compressor compressor-id)
                              compressor)
                 encryptor  (if (identical? encryptor :auto)
                              (get-auto-encryptor encryptor-id)
                              encryptor)]

             (when (and encryptor (not password))
               (ex "Password required for decryption."))

             (try
               (let [ba data-ba
                     ba (if encryptor  (decrypt    encryptor password ba) ba)
                     ba (if compressor (decompress compressor         ba) ba)
                     dis (DataInputStream. (ByteArrayInputStream. ba))]

                 (thaw-from-in! dis))

               (catch Exception e (ex-fn e)))))

         ;; Hackish + can actually segfault JVM due to Snappy bug,
         ;; Ref. http://goo.gl/mh7Rpy - no better alternatives, unfortunately
         thaw-v1-data
         (fn [data-ba ex-fn]
           (thaw-data data-ba :snappy nil
             (fn [_] (thaw-data data-ba nil nil (fn [_] (ex-fn nil))))))]

     (if no-header?
       (if v2+?
         (thaw-data ba :no-header :no-header (fn [e] (ex e err-msg-unknown-thaw-failure)))
         (thaw-data ba :no-header :no-header
           (fn [e] (thaw-v1-data ba (fn [_] (ex e err-msg-unknown-thaw-failure))))))

       ;; At this point we assume that we have a header iff we have v2+ data
       (if-let [[data-ba {:keys [compressor-id encryptor-id unrecognized-meta?]
                          :as   head-meta}] (try-parse-header ba)]

         ;; A well-formed header _appears_ to be present (it's possible though
         ;; unlikely that this is a fluke and data is actually headerless):
         (if v2+?
           (if unrecognized-meta?
             (ex err-msg-unrecognized-header)
             (thaw-data data-ba compressor-id encryptor-id
               (fn [e] (ex e err-msg-unknown-thaw-failure))))

           (if unrecognized-meta?
             (thaw-v1-data ba (fn [_] (ex err-msg-unrecognized-header)))
             (thaw-data data-ba compressor-id encryptor-id
               (fn [e] (thaw-v1-data ba (fn [_] (ex e err-msg-unknown-thaw-failure)))))))

         ;; Well-formed header definitely not present
         (if v2+?
           (ex err-msg-unknown-thaw-failure)
           (thaw-v1-data ba (fn [_] (ex err-msg-unknown-thaw-failure)))))))))
