(ns clara.rules.platform
  "This namespace is for internal use and may move in the future.
   Platform unified code Clojure/ClojureScript.")

(defn throw-error
  "Throw an error with the given description string."
  [^String description]
  (throw #?(:clj (IllegalArgumentException. description) :cljs (js/Error. description))))

#?(:clj
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
                                            xs (or (.get m k)
                                                   (transient []))]
                                        (.put m k (conj! xs x)))
                                      m)
                                    (java.util.LinkedHashMap.)
                                    coll)
           it (.iterator (.entrySet m))]
       ;; Explicitly iterate over a Java iterator in order to avoid running into issues as
       ;; discussed in http://dev.clojure.org/jira/browse/CLJ-1738
       (loop [coll (transient [])]
         (if (.hasNext it)
           (let [^java.util.Map$Entry e (.next it)]
             (recur (conj! coll [(.getKey e) (persistent! (.getValue e))])))
           (persistent! coll)))))
   :cljs
   (def group-by-seq (comp seq clojure.core/group-by)))

#?(:clj
    (defn tuned-group-by
      "Equivalent of the built-in group-by, but tuned for when there are many values per key."
      [f coll]
      (->> coll
           (reduce (fn [map value]
                     (let [k (f value)
                           items (or (.get ^java.util.HashMap map k)
                                     (transient []))]
                       (.put ^java.util.HashMap map k (conj! items value)))
                     map)
                   (java.util.HashMap.))
          (reduce (fn [map [key value]]
                      (assoc! map key (persistent! value)))
                    (transient {}))
          (persistent!)))
    :cljs
    (def tuned-group-by clojure.core/group-by))

#?(:clj
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
                 `(.remove ~tl)))))))