(ns clara.rules.hierarchy
  (:refer-clojure :exclude [derive
                            underive
                            make-hierarchy])
  (:require [clojure.core :as core]))

(defn make-hierarchy
  []
  (-> (core/make-hierarchy)
      (assoc :hierarchy-data [])))

(def ^:dynamic *hierarchy* nil)

(defn- derive*
  [h tag parent]
  (assert (some? h))
  (assert (some? tag))
  (assert (some? parent))
  (assert (not= tag parent))
  (let [tp (:parents h)
        td (:descendants h)
        ta (:ancestors h)
        tf (fn do-transform
             [m source sources target targets]
             (reduce (fn [ret k]
                       (assoc ret k
                              (reduce conj (get targets k #{}) (cons target (targets target)))))
                     m (cons source (sources source))))]
    (or
     (when-not (contains? (tp tag) parent)
       (when (contains? (ta tag) parent)
         h)
       (when (contains? (ta parent) tag)
         (throw (Exception. (print-str "Cyclic derivation:" parent "has" tag "as ancestor"))))
       (-> (assoc-in h [:parents tag] (conj (get tp tag #{}) parent))
           (update :ancestors tf tag td parent ta)
           (update :descendants tf parent ta tag td)))
     h)))

(defn- underive*
  [h tag parent]
  (assert (some? h))
  (assert (some? tag))
  (assert (some? parent))
  (assert (not= tag parent))
  (let [parent-map (:parents h)
        childs-parents (if (parent-map tag)
                         (disj (parent-map tag) parent) #{})
        new-parents (if (not-empty childs-parents)
                      (assoc parent-map tag childs-parents)
                      (dissoc parent-map tag))
        deriv-seq (map #(cons (key %) (interpose (key %) (val %)))
                       (seq new-parents))]
    (if (contains? (parent-map tag) parent)
      (reduce (fn do-derive
                [h [t p]]
                (derive* h t p)) (make-hierarchy)
              deriv-seq)
      h)))

(defn derive
  "Establishes a parent/child relationship between parent and
  tag. Both tag and parent cannot be null, h must be a hierarchy obtained from make-hierarchy.
  Unlike `clojure.core/underive`, there is no restriction
  on the type of values that tag and parent can be.
  When only two tag and parent are passed, this function modifies the *hierarchy* atom."
  ([tag parent]
   (assert *hierarchy* "*hierarchy* must be bound")
   (swap! *hierarchy* derive tag parent))
  ([h tag parent]
   (-> (derive* h tag parent)
       (update :hierarchy-data conj [:d tag parent]))))

(defn underive
  "Removes a parent/child relationship between parent and
   tag. h must be a hierarchy obtained from make-hierarchy.
   Unlike `clojure.core/underive`, there is no restriction
   on the type of values that tag and parent can be.
   When only two tag and parent are passed, this function modifies the *hierarchy* atom."
  ([tag parent]
   (assert *hierarchy* "*hierarchy* must be bound")
   (swap! *hierarchy* underive tag parent))
  ([h tag parent]
   (-> (underive* h tag parent)
       (update :hierarchy-data conj [:u tag parent]))))
