(ns clara.rules.durability.fressian
  "A default Fressian-based implementation of d/ISessionSerializer.

   Note!  Currently this only supports the clara.rules.memory.PersistentLocalMemory implementation
          of memory."
  (:require [clara.rules.durability :as d]
            [clara.rules.memory :as mem]
            [clara.rules.engine :as eng]
            [clara.rules.compiler :as com]
            [clara.rules.platform :as pform]
            [schema.core :as s]
            [clojure.data.fressian :as fres]
            [clojure.java.io :as jio]
            [clojure.main :as cm])
  (:import [clara.rules.durability
            MemIdx
            InternalMemIdx]
           [clara.rules.memory
            RuleOrderedActivation]
           [clara.rules.engine
            Token
            Element
            ProductionNode
            QueryNode
            AlphaNode
            RootJoinNode
            HashJoinNode
            ExpressionJoinNode
            NegationNode
            NegationWithJoinFilterNode
            TestNode
            AccumulateNode
            AccumulateWithJoinFilterNode]
           [org.fressian
            StreamingWriter
            Writer
            Reader
            FressianWriter
            FressianReader]
           [org.fressian.handlers
            WriteHandler
            ReadHandler]
           [java.util
            ArrayList
            IdentityHashMap
            Map
            WeakHashMap]
           [java.io
            InputStream
            OutputStream]))

;; Use this map to cache the symbol for the map->RecordNameHere
;; factory function created for every Clojure record to improve
;; serialization performance.
;; See https://github.com/cerner/clara-rules/issues/245 for more extensive discussion.
(def ^:private ^Map class->factory-fn-sym (WeakHashMap.))

(defn record-map-constructor-name
  "Return the 'map->' prefix, factory constructor function for a Clojure record."
  [rec]
  (let [klass (class rec)]
    (if-let [cached-sym (.get class->factory-fn-sym klass)]
      cached-sym
      (let [class-name (.getName ^Class klass)
            idx (.lastIndexOf class-name (int \.))
            ns-nom (.substring class-name 0 idx)
            nom (.substring class-name (inc idx))
            factory-fn-sym (symbol (str (cm/demunge ns-nom)
                                        "/map->"
                                        (cm/demunge nom)))]
        (.put class->factory-fn-sym klass factory-fn-sym)
        factory-fn-sym))))

(defn write-map
  "Writes a map as Fressian with the tag 'map' and all keys cached."
  [^Writer w m]
  (.writeTag w "map" 1)
  (.beginClosedList ^StreamingWriter w)
  (reduce-kv
   (fn [^Writer w k v]
     (.writeObject w k true)
     (.writeObject w v))
   w
   m)
  (.endList ^StreamingWriter w))

(defn write-with-meta
  "Writes the object to the writer under the given tag.  If the record has metadata, the metadata
   will also be written.  read-with-meta will associated this metadata back with the object
   when reading."
  ([w tag o]
   (write-with-meta w tag o (fn [^Writer w o] (.writeList w o))))
  ([^Writer w tag o write-fn]
   (let [m (meta o)]
     (do
       (.writeTag w tag 2)
       (write-fn w o)
       (if m
         (.writeObject w m)
         (.writeNull w))))))

(defn- read-meta [^Reader rdr]
  (some->> rdr
           .readObject
           (into {})))

(defn read-with-meta
  "Reads an object from the reader that was written via write-with-meta.  If the object was written
   with metadata the metadata will be associated on the object returned."
  [^Reader rdr build-fn]
  (let [o (build-fn (.readObject rdr))
        m (read-meta rdr)]
    (cond-> o
      m (with-meta m))))

(defn write-record
  "Same as write-with-meta, but with Clojure record support.  The type of the record will
   be preserved."
  [^Writer w tag rec]
  (let [m (meta rec)]
    (.writeTag w tag 3)
    (.writeObject w (record-map-constructor-name rec) true)
    (write-map w rec)
    (if m
      (.writeObject w m)
      (.writeNull w))))

(defn read-record
  "Same as read-with-meta, but with Clojure record support.  The type of the record will
   be preserved."
  ([^Reader rdr]
   (read-record rdr nil))
  ([^Reader rdr add-fn]
   (let [builder (-> (.readObject rdr) resolve deref)
         build-map (.readObject rdr)
         m (read-meta rdr)]
     (cond-> (builder build-map)
       m (with-meta m)
       add-fn add-fn))))

(defn- create-cached-node-handler
  ([clazz
    tag
    tag-for-cached]
   {:class clazz
    :writer (reify WriteHandler
              (write [_ w o]
                (let [node-id (:id o)]
                  (if (@(.get d/node-id->node-cache) node-id)
                    (do
                      (.writeTag w tag-for-cached 1)
                      (.writeInt w node-id))
                    (do
                      (d/cache-node o)
                      (write-record w tag o))))))
    :readers {tag-for-cached
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (d/node-id->node (.readObject rdr))))
              tag
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (-> rdr
                      read-record
                      d/cache-node)))}})
  ([clazz
    tag
    tag-for-cached
    remove-node-expr-fn
    add-node-expr-fn]
   {:class clazz
    :writer (reify WriteHandler
              (write [_ w o]
                (let [node-id (:id o)]
                  (if (@(.get d/node-id->node-cache) node-id)
                    (do
                      (.writeTag w tag-for-cached 1)
                      (.writeInt w node-id))
                    (do
                      (d/cache-node o)
                      (write-record w tag (remove-node-expr-fn o)))))))
    :readers {tag-for-cached
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (d/node-id->node (.readObject rdr))))
              tag
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (-> rdr
                      (read-record add-node-expr-fn)
                      d/cache-node)))}}))

(def handlers
  "A structure tying together the custom Fressian write and read handlers used
   by FressianSessionSerializer's."
  {"java/class"
   {:class Class
    :writer (reify WriteHandler
              (write [_ w c]
                (.writeTag w "java/class" 1)
                (.writeObject w (symbol (.getName ^Class c)) true)))
    :readers {"java/class"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (resolve (.readObject rdr))))}}

   "clj/set"
   {:class clojure.lang.APersistentSet
    :writer (reify WriteHandler
              (write [_ w o]
                (write-with-meta w "clj/set" o)))
    :readers {"clj/set"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (read-with-meta rdr set)))}}

   "clj/vector"
   {:class clojure.lang.APersistentVector
    :writer (reify WriteHandler
              (write [_ w o]
                (write-with-meta w "clj/vector" o)))
    :readers {"clj/vector"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (read-with-meta rdr vec)))}}

   "clj/list"
   {:class clojure.lang.PersistentList
    :writer (reify WriteHandler
              (write [_ w o]
                (write-with-meta w "clj/list" o)))
    :readers {"clj/list"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (read-with-meta rdr #(apply list %))))}}

   "clj/aseq"
   {:class clojure.lang.ASeq
    :writer (reify WriteHandler
              (write [_ w o]
                (write-with-meta w "clj/aseq" o)))
    :readers {"clj/aseq"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (read-with-meta rdr sequence)))}}

   "clj/lazyseq"
   {:class clojure.lang.LazySeq
    :writer (reify WriteHandler
              (write [_ w o]
                (write-with-meta w "clj/lazyseq" o)))
    :readers {"clj/lazyseq"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (read-with-meta rdr sequence)))}}

   "clj/map"
   {:class clojure.lang.APersistentMap
    :writer (reify WriteHandler
              (write [_ w o]
                (write-with-meta w "clj/map" o write-map)))
    :readers {"clj/map"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (read-with-meta rdr #(into {} %))))}}
   
   "clj/treeset"
   {:class clojure.lang.PersistentTreeSet
    :writer (reify WriteHandler
              (write [_ w o]
                (let [cname (d/sorted-comparator-name o)]
                  (.writeTag w "clj/treeset" 3)
                  (if cname
                    (.writeObject w cname true)
                    (.writeNull w))
                  ;; Preserve metadata.
                  (if-let [m (meta o)]
                    (.writeObject w m)
                    (.writeNull w))
                  (.writeList w o))))
    :readers {"clj/treeset"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (let [c (some-> rdr .readObject resolve deref)
                        m (.readObject rdr)
                        s (-> (.readObject rdr)
                              (d/seq->sorted-set c))]
                    (if m
                      (with-meta s m)
                      s))))}}
   
   "clj/treemap"
   {:class clojure.lang.PersistentTreeMap
    :writer (reify WriteHandler
              (write [_ w o]
                (let [cname (d/sorted-comparator-name o)]
                  (.writeTag w "clj/treemap" 3)
                  (if cname
                    (.writeObject w cname true)
                    (.writeNull w))
                  ;; Preserve metadata.
                  (if-let [m (meta o)]
                    (.writeObject w m)
                    (.writeNull w))
                  (write-map w o))))
    :readers {"clj/treemap"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (let [c (some-> rdr .readObject resolve deref)
                        m (.readObject rdr)
                        s (d/seq->sorted-map (.readObject rdr) c)]
                    (if m
                      (with-meta s m)
                      s))))}}

   "clj/mapentry"
   {:class clojure.lang.MapEntry
    :writer (reify WriteHandler
              (write [_ w o]
                (.writeTag w "clj/mapentry" 2)
                (.writeObject w (key o) true)
                (.writeObject w (val o))))
    :readers {"clj/mapentry"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (d/create-map-entry (.readObject rdr)
                                      (.readObject rdr))))}}

   ;; Have to redefine both Symbol and IRecord to support metadata as well
   ;; as identity-based caching for the IRecord case.

   "clj/sym"
   {:class clojure.lang.Symbol
    :writer (reify WriteHandler
              (write [_ w o]
                ;; Mostly copied from private fres/write-named, except the metadata part.
                (.writeTag w "clj/sym" 3)
                (.writeObject w (namespace o) true)
                (.writeObject w (name o) true)
                (if-let [m (meta o)]
                  (.writeObject w m)
                  (.writeNull w))))
    :readers {"clj/sym"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (let [s (symbol (.readObject rdr) (.readObject rdr))
                        m (read-meta rdr)]
                    (cond-> s
                      m (with-meta m)))))}}

   "clj/record"
   {:class clojure.lang.IRecord
    ;; Write a record a single time per object reference to that record.  The record is then "cached"
    ;; with the IdentityHashMap `d/clj-record-holder`.  If another reference to this record instance
    ;; is encountered later, only the "index" of the record in the map will be written.
    :writer (reify WriteHandler
              (write [_ w rec]
                (if-let [idx (d/clj-record-fact->idx rec)]
                  (do
                    (.writeTag w "clj/recordidx" 1)
                    (.writeInt w idx))
                  (do
                    (write-record w "clj/record" rec)
                    (d/clj-record-holder-add-fact-idx! rec)))))
    ;; When reading the first time a reference to a record instance is found, the entire record will
    ;; need to be constructed.  It is then put into indexed cache.  If more references to this record
    ;; instance are encountered later, they will be in the form of a numeric index into this cache.
    ;; This is guaranteed by the semantics of the corresponding WriteHandler.
    :readers {"clj/recordidx"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (d/clj-record-idx->fact (.readInt rdr))))
              "clj/record"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (-> rdr
                      read-record
                      d/clj-record-holder-add-fact!)))}}

   "clara/productionnode"
   (create-cached-node-handler ProductionNode
                               "clara/productionnode"
                               "clara/productionnodeid"
                               #(assoc % :rhs nil)
                               d/add-rhs-fn)

   "clara/querynode"
   (create-cached-node-handler QueryNode
                               "clara/querynode"
                               "clara/querynodeid")

   "clara/alphanode"
   {:class AlphaNode
    ;; The writer and reader here work similar to the IRecord implementation.  The only
    ;; difference is that the record needs to be written with out the compiled clj
    ;; function on it.  This is due to clj functions not having any convenient format
    ;; for serialization.  The function is restored by re-eval'ing the function based on
    ;; its originating code form at read-time.
    :writer (reify WriteHandler
              (write [_ w o]
                (if-let [idx (d/clj-record-fact->idx o)]
                  (do
                    (.writeTag w "clara/alphanodeid" 1)
                    (.writeInt w idx))
                  (do
                    (write-record w "clara/alphanode" (assoc o :activation nil))
                    (d/clj-record-holder-add-fact-idx! o)))))
    :readers {"clara/alphanodeid"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (d/clj-record-idx->fact (.readObject rdr))))
              "clara/alphanode"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (-> rdr
                      (read-record d/add-alpha-fn)
                      d/clj-record-holder-add-fact!)))}}

   "clara/rootjoinnode"
   (create-cached-node-handler RootJoinNode
                               "clara/rootjoinnode"
                               "clara/rootjoinnodeid")

   "clara/hashjoinnode"
   (create-cached-node-handler HashJoinNode
                               "clara/hashjoinnode"
                               "clara/hashjoinnodeid")

   "clara/exprjoinnode"
   (create-cached-node-handler ExpressionJoinNode
                               "clara/exprjoinnode"
                               "clara/exprjoinnodeid"
                               #(assoc % :join-filter-fn nil)
                               d/add-join-filter-fn)

   "clara/negationnode"
   (create-cached-node-handler NegationNode
                               "clara/negationnode"
                               "clara/negationnodeid")

   "clara/negationwjoinnode"
   (create-cached-node-handler NegationWithJoinFilterNode
                               "clara/negationwjoinnode"
                               "clara/negationwjoinnodeid"
                               #(assoc % :join-filter-fn nil)
                               d/add-join-filter-fn)

   "clara/testnode"
   (create-cached-node-handler TestNode
                               "clara/testnode"
                               "clara/testnodeid"
                               #(assoc % :test nil)
                               d/add-test-fn)

   "clara/accumnode"
   (create-cached-node-handler AccumulateNode
                               "clara/accumnode"
                               "clara/accumnodeid"
                               #(assoc % :accumulator nil)
                               d/add-accumulator)

   "clara/accumwjoinnode"
   (create-cached-node-handler AccumulateWithJoinFilterNode
                               "clara/accumwjoinnode"
                               "clara/accumwjoinnodeid"
                               #(assoc % :accumulator nil :join-filter-fn nil)
                               (comp d/add-accumulator d/add-join-filter-fn))

   "clara/ruleorderactivation"
   {:class RuleOrderedActivation
    :writer (reify WriteHandler
              (write [_ w c]
                (.writeTag w "clara/ruleorderactivation" 4)
                (.writeObject w (.-node-id ^RuleOrderedActivation c) true)
                (.writeObject w (.-token ^RuleOrderedActivation c))
                (.writeObject w (.-activation ^RuleOrderedActivation c))
                (.writeInt w (.-rule-load-order ^RuleOrderedActivation c))))
    :readers {"clara/ruleorderactivation"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (mem/->RuleOrderedActivation (.readObject rdr)
                                               (.readObject rdr)
                                               (.readObject rdr)
                                               (.readObject rdr))))}}

   "clara/memidx"
   {:class MemIdx
    :writer (reify WriteHandler
              (write [_ w c]
                (.writeTag w "clara/memidx" 1)
                (.writeInt w (:idx c))))
    :readers {"clara/memidx"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (d/find-mem-idx (.readObject rdr))))}}

   "clara/internalmemidx"
   {:class InternalMemIdx
    :writer (reify WriteHandler
              (write [_ w c]
                (.writeTag w "clara/internalmemidx" 1)
                (.writeInt w (:idx c))))
    :readers {"clara/internalmemidx"
              (reify ReadHandler
                (read [_ rdr tag component-count]
                  (d/find-internal-idx (.readObject rdr))))}}})

(def write-handlers
  "All Fressian write handlers used by FressianSessionSerializer's."
  (into fres/clojure-write-handlers
        (map (fn [[tag {clazz :class wtr :writer}]]
               [clazz {tag wtr}]))
        handlers))

(def read-handlers
  "All Fressian read handlers used by FressianSessionSerializer's."
    (->> handlers
       vals
       (into fres/clojure-read-handlers
             (mapcat :readers))))

(def write-handler-lookup
  (-> write-handlers
      fres/associative-lookup
      fres/inheritance-lookup))

(def read-handler-lookup
  (fres/associative-lookup read-handlers))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;; Session serializer.
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord FressianSessionSerializer [in-stream out-stream]
  d/ISessionSerializer
  (serialize [_ session opts]
    (let [{:keys [rulebase memory]} (eng/components session)
          rulebase (assoc rulebase
                          :activation-group-sort-fn nil
                          :activation-group-fn nil
                          :get-alphas-fn nil)
          record-holder (IdentityHashMap.)
          do-serialize
          (fn [sources]
            (with-open [^FressianWriter wtr
                        (fres/create-writer out-stream :handlers write-handler-lookup)]
              (pform/thread-local-binding [d/node-id->node-cache (volatile! {})
                                           d/clj-record-holder record-holder]
                                          (doseq [s sources] (fres/write-object wtr s)))))]
      
      ;; In this case there is nothing to do with memory, so just serialize immediately.
      (if (:rulebase-only? opts)
        (do-serialize [rulebase])
        
        ;; Otherwise memory needs to have facts extracted to return.
        (let [{:keys [memory indexed-facts internal-indexed-facts]} (d/indexed-session-memory-state memory)
              sources (if (:with-rulebase? opts)
                        [rulebase internal-indexed-facts memory]
                        [internal-indexed-facts memory])]
          
          (do-serialize sources)
          
          ;; Return the facts needing to be serialized still.
          indexed-facts))))
  
  (deserialize [_ mem-facts opts]
    
    (with-open [^FressianReader rdr (fres/create-reader in-stream :handlers read-handler-lookup)]
      (let [{:keys [rulebase-only? base-rulebase]} opts
            
            record-holder (ArrayList.)
            ;; The rulebase should either be given from the base-session or found in
            ;; the restored session-state.
            maybe-base-rulebase (when (and (not rulebase-only?) base-rulebase)
                                  base-rulebase)
            rulebase (if maybe-base-rulebase
                       maybe-base-rulebase
                       (let [without-opts-rulebase (pform/thread-local-binding [d/node-id->node-cache (volatile! {})
                                                                                d/compile-expr-fn (memoize (fn [id expr] (com/try-eval expr)))
                                                                                d/clj-record-holder record-holder]
                                                                               (fres/read-object rdr))]
                         (d/rulebase->rulebase-with-opts without-opts-rulebase opts)))]
        
        (if rulebase-only?
          rulebase
          (d/assemble-restored-session rulebase
                                       (pform/thread-local-binding [d/clj-record-holder record-holder
                                                                    d/mem-facts mem-facts]
                                                                   ;; internal memory contains facts provided by mem-facts
                                                                   ;; thus mem-facts must be bound before the call to read
                                                                   ;; the internal memory
                                                                   (pform/thread-local-binding [d/mem-internal (fres/read-object rdr)]
                                                                                               (fres/read-object rdr)))
                                       opts))))))

(s/defn create-session-serializer
  "Creates an instance of FressianSessionSerializer which implements d/ISessionSerializer by using
   Fressian serialization for the session structures.
   
   In the one arity case, takes either an input stream or an output stream.  This arity is intended for
   creating a Fressian serializer instance that will only be used for serialization or deserialization,
   but not both.  e.g. This is often convenient if serialization and deserialization are not done from
   the same process.  If it is to be used for serialization, then the stream given should be an output
   stream.  If it is to be used for deserialization, then the stream to be given should be an
   input stream.

   In the two arity case, takes an input stream and an output stream.  These will be used for
   deserialization and serialization within the created Fressian serializer instance, respectively.

   Note!  Currently this only supports the clara.rules.memory.PersistentLocalMemory implementation
          of memory."
  ([in-or-out-stream :- (s/pred (some-fn #(instance? InputStream %)
                                         #(instance? OutputStream %))
                                "java.io.InputStream or java.io.OutputStream")]
   (if (instance? InputStream in-or-out-stream)
     (create-session-serializer in-or-out-stream nil)
     (create-session-serializer nil in-or-out-stream)))

  ([in-stream :- (s/maybe InputStream)
    out-stream :- (s/maybe OutputStream)]
   (->FressianSessionSerializer in-stream out-stream)))
