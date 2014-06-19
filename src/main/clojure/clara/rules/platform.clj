(ns clara.rules.platform
  "Code specific to the JVM platform.")

(defn throw-error
  "Throw an error with the given description string."
  [description]
  (throw (IllegalArgumentException. description)))

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
