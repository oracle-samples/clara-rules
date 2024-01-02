(ns clara.rules.platform
  "This namespace is for internal use and may move in the future.
  Platform unified code Clojure/ClojureScript."
  (:require [futurama.core :refer [!<!
                                   !<!!
                                   async
                                   async?
                                   completable-future]])
  (:import [java.lang IllegalArgumentException]
           [java.util LinkedHashMap]))

(defn throw-error
  "Throw an error with the given description string."
  [^String description]
  (throw (IllegalArgumentException. description)))

(defn query-param
  "Coerces a query param to a parameter keyword such as :?param, if an unsupported type is
  supplied then an exception will be thrown"
  [p]
  (cond
    (keyword? p) p
    (symbol? p) (keyword p)
    :else
    (throw-error (str "Query bindings must be specified as a keyword or symbol: " p))))

;; This class wraps Clojure objects to ensure Clojure's equality and hash
;; semantics are visible to Java code. This allows these Clojure objects
;; to be safely used in things like Java Sets or Maps.
;; This class also accepts and stores the hash code, since it almost always
;; will be used once and generally more than once.
(deftype JavaEqualityWrapper [wrapped ^int hash-code]

  Object
  (equals [this other]

    (cond

      ;; There are some cases where the inserted and retracted facts could be identical, particularly
      ;; if user code in the RHS has caches, so we go ahead and check for identity as a first-pass check,
      ;; but there are probably aren't enough cases where the facts are identical to make doing a full sweep
      ;; on identity first worthwhile, particularly since in practice the hash check will make the vast majority
      ;; of .equals calls that return false quite fast.
      (identical? wrapped (.wrapped ^JavaEqualityWrapper other))
      true

      (not (== hash-code (.hash_code ^JavaEqualityWrapper other)))
      false

      :else (= wrapped (.wrapped ^JavaEqualityWrapper other))))

  (hashCode [this] hash-code))

(defn jeq-wrap
  "wraps the value with a JavaEqualityWrapper"
  ^JavaEqualityWrapper [value]
  (JavaEqualityWrapper. value (hash value)))

(defn group-by-seq
  "Groups the items of the given coll by f to each item.  Returns a seq of tuples of the form
  [f-val xs] where xs are items from the coll and f-val is the result of applying f to any of
  those xs.  Each x in xs has the same value (f x).  xs will be in the same order as they were
  found in coll.
  The behavior is similar to calling `(seq (group-by f coll))` However, the returned seq will
  always have consistent ordering from process to process.  The ordering is insertion order
  as new (f x) values are found traversing the given coll collection in its seq order.  The
  returned order is made consistent to ensure that relevant places within the rules engine that
  use this grouping logic have deterministic behavior across different processes."
  [f coll]
  (let [^java.util.Map m (reduce (fn [^java.util.Map m x]
                                   (let [k (f x)
                                         ;; Use Java's hashcode for performance reasons as
                                         ;; discussed at https://github.com/cerner/clara-rules/issues/393
                                         wrapper (jeq-wrap k)
                                         xs (or (.get m wrapper)
                                                (transient []))]
                                     (.put m wrapper (conj! xs x)))
                                   m)
                                 (LinkedHashMap.)
                                 coll)
        it (.iterator (.entrySet m))]
    ;; Explicitly iterate over a Java iterator in order to avoid running into issues as
    ;; discussed in http://dev.clojure.org/jira/browse/CLJ-1738
    (loop [coll (transient [])]
      (if (.hasNext it)
        (let [^java.util.Map$Entry e (.next it)]
          (recur (conj! coll [(.wrapped ^JavaEqualityWrapper (.getKey e)) (persistent! (.getValue e))])))
        (persistent! coll)))))

(defmacro thread-local-binding
  "Wraps given body in a try block, where it sets each given ThreadLocal binding
  and removes it in finally block."
  [bindings & body]
  (when-not (vector? bindings)
    (throw (ex-info "Binding needs to be a vector."
                    {:bindings bindings})))
  (when-not (even? (count bindings))
    (throw (ex-info "Needs an even number of forms in binding vector"
                    {:bindings bindings})))
  (let [binding-pairs (partition 2 bindings)]
    `(try
       ~@(for [[tl v] binding-pairs]
           `(.set ~tl ~v))
       ~@body
       (finally
         ~@(for [[tl] binding-pairs]
             `(.remove ~tl))))))

(defmacro eager-for
  "A for wrapped with a doall to force realisation. Usage is the same as regular for."
  [& body]
  `(doall (for ~@body)))

(def ^:dynamic *parallel-compute* false)

(defmacro compute-for
  [bindings body]
  `(if *parallel-compute*
     (let [fut-seq# (eager-for
                     [~@bindings]
                     (completable-future
                      ~body))]
       (!<!!
        (async
         (loop [[res-fut# & more#] fut-seq#
                results# []]
           (if (nil? res-fut#)
             results#
             (recur more#
                    (if-let [result# (!<! res-fut#)]
                      (conj results# result#)
                      results#)))))))
     (eager-for
      [~@bindings
       :let [result# ~body]
       :when result#]
      result#)))

(def ^:dynamic *production* nil)

(defmacro produce-try
  [body & catch-finally]
  `(if *parallel-compute*
     (let [result# (try
                     ~body
                     ~@catch-finally)]
       (if (async? result#)
         (async
          (try
            (!<! result#)
            ~@catch-finally))
         result#))
     (try
       ~body
       ~@catch-finally)))

(defmacro produce-for
  [bindings prod-body & post-body]
  `(if *parallel-compute*
     (let [res-seq# (eager-for
                     [~@bindings]
                     (async
                      (let [result# (!<! ~prod-body)]
                        (if ~(some? post-body)
                          (binding [*production* result#]
                            ~@post-body)
                          result#))))]
       (!<!!
        (async
         (loop [[result# & more#] res-seq#
                results# []]
           (if-not result#
             results#
             (recur more# (conj results# (!<! result#))))))))
     (eager-for
      [~@bindings
       :let [result# ~prod-body]]
      (if ~(some? post-body)
        (binding [*production* result#]
          ~@post-body)
        result#))))
