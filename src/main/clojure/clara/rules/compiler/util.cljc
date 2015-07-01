(ns clara.rules.compiler.util)

(defn gen-compare
  "Generic compare function for arbitrary Clojure data structures.
   The only guarantee is the ordering of items will be consistent
   between invocations."
  [left right]
  (cond

   ;; Handle the nil cases first to ensure nil-safety.
   (and (nil? left) (nil? right))
   0

   (nil? left)
   -1

   (nil? right)
   1
    
   ;; Ignore functions for our comparison purposes,
   ;; since we won't distinguish rules by them.
   (and (fn? left) (fn? right))
   0

   ;; If the types differ, compare based on their names.
   (not= (type left)
         (type right))
   (compare (.getName ^Class (type left))
            (.getName ^Class (type right)))
   
   ;; Compare items in a sequence until we find a difference.
   (sequential? left)
   (loop [left-seq left
          right-seq right]

     (if (and (seq left-seq) (seq right-seq))

       ;; Both sequences have content, so compare them and recur if necessary.
       (let [result (gen-compare (first left-seq) (first right-seq)) ]
         (if (not= 0 result)
           result
           (recur (rest left-seq) (rest right-seq))))

       ;; At least one sequence is empty.
       (cond

        (seq left-seq) 1
        (seq right-seq) -1
        :default 0)))

   ;; Covert maps to sequences sorted by keys and compare those sequences.
   (map? left)
   (let [kv-sort-fn (fn [[key1 _] [key2 _]] (gen-compare key1 key2))
         left-kvs (sort kv-sort-fn (seq left))
         right-kvs (sort kv-sort-fn (seq right))  ]

     (gen-compare left-kvs right-kvs))

   ;; The content is comparable and not a sequence, so simply compare them.
   #?@(:clj
        [(instance? Comparable left)
         (compare left right)])
   (string? left) (= left right)
   (number? left) (= left right)

   ;; Unknown items are just treated as equal for our purposes,
   ;; since we can't define an ordering.
   :default 0))
