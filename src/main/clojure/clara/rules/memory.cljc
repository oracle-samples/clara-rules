(ns clara.rules.memory
  "Specification and default implementation of working memory"
  (:require [clojure.set :as s]))

;; Activation record used by get-activations and add-activations! below.
(defrecord Activation [node token])

(defprotocol IPersistentMemory
  (to-transient [memory]))

(defprotocol IMemoryReader
  ;; Returns the rulebase associated with the given memory.
  (get-rulebase [memory])

  ;; Returns the elements assoicated with the given node.
  (get-elements [memory node bindings])

  ;; Returns all elements associated with the given node, regardless of bindings.
  (get-elements-all [memory node])

  ;; Returns the tokens associated with the given node.
  (get-tokens [memory node bindings])

  ;; Returns all tokens associated with the given node, regardless of bindings
  (get-tokens-all [memory node])

  ;; Returns the reduced form of objects processed by an accumulator node
  ;; for facts that match the given bindings.
  (get-accum-reduced [memory node join-bindings fact-bindings])

  ;; Returns all reduced results for the given node that matches
  ;; the given join bindings, independent of the individual fact-bindings
  ;; created by the accumulator's condition.
  (get-accum-reduced-all [memory node join-bindings])

  ;; Returns a tuple of [join-bindings fact-bindings result] for all
  ;; accumulated items on this node.
  (get-accum-reduced-complete [memory node])

  ;; Returns insertions that occurred at the given node.
  (get-insertions [memory node token])

  ;; Returns a map of nodes with pending activations to the activations themselves.
  (get-activations [memory]))

(defprotocol ITransientMemory

  ;; Adds working memory elements to the given working memory at the given node.
  (add-elements! [memory node join-bindings elements])

  ;; Remove working memory elements from the given working memory at the given node.
  (remove-elements! [memory node elements join-bindings])

  ;; Add tokens to the given working memory at the given node.
  (add-tokens! [memory node join-bindings tokens])

  ;; Removes tokens from the given working memory at the given node.
  (remove-tokens! [memory node join-bindings tokens])

  ;; Adds the result of a reduced accumulator execution to the given memory and node.
  (add-accum-reduced! [memory node join-bindings accum-result fact-bindings])

  ;; Add a record that a given fact twas inserted at a given node with
  ;; the given support. Used for truth maintenance.
  (add-insertions! [memory node token facts])

  ;; Removes all records of facts that were inserted at the given node
  ;; due to the given token. Used for truth maintenance.
  (remove-insertions! [memory node token])

  ;; Add a sequence of activations.
  (add-activations! [memory production activations])

  ;; Pop an activation from the working memory. Returns nil if no
  ;; activations are pending.
  (pop-activation! [memory])

  ;; Returns the group of the next activation, or nil if none are pending.
  (next-activation-group [memory])

  ;; Remove the given activations from the working memory.
  (remove-activations! [memory production activations])

  ;; Clear all activations from the working memory
  (clear-activations! [memory])

  ;; Converts the transient memory to persistent form.
  (to-persistent! [memory]))

(defn remove-first-of-each
  "Remove the first instance of each item in the given remove-seq that
  appears in the collection.  This also tracks which items were found
  and removed.  Returns a tuple of the form:
  [items-removed coll-with-items-removed]
  This function does so eagerly since
  the working memories with large numbers of insertions and retractions
  can cause lazy sequences to become deeply nested."
  [remove-seq coll]
  (cond

    ;; There is nothing to remove.
    (empty? remove-seq) [[] coll]

    ;; Optimization for special case of one item to remove,
    ;; which occurs frequently.
    (= 1 (count remove-seq))

    (let [item-to-remove (first remove-seq)
          [before-it [it & after-it]] (split-with #(not= item-to-remove %) coll)
          removed (if it [it] [])]
      [removed (into before-it after-it)])

    ;; Otherwise, perform a linear search for items to remove.
    :else (loop [f (first coll)
                 r (rest coll)
                 [remove-seq items-removed result] [remove-seq (transient []) (transient [])]]

            (if f
              (recur (first r)
                     (rest r)

                     ;; Determine if f matches any of the items to remove.
                     (loop [to-remove (first remove-seq)
                            remove-seq (rest remove-seq)
                            ;; Remember what is left to remove for later.
                            left-to-remove (transient [])]

                       ;; Try to find if f matches anything to-remove.
                       (if to-remove
                         (if (= to-remove f)

                           ;; Found a match, so the search is done.
                           [(persistent! (reduce conj! left-to-remove remove-seq))
                            (conj! items-removed to-remove)
                            result]

                           ;; Keep searching for a match.
                           (recur (first remove-seq)
                                  (rest remove-seq)
                                  (conj! left-to-remove to-remove)))

                         ;; No matches found.
                         [(persistent! left-to-remove)
                          items-removed
                          (conj! result f)])))

              [(persistent! items-removed) (persistent! result)]))))

(declare ->PersistentLocalMemory)

;;; Transient local memory implementation. Typically only persistent memory will be visible externally.

(deftype TransientLocalMemory [rulebase
                               activation-group-sort-fn
                               activation-group-fn
                               ^:unsynchronized-mutable alpha-memory
                               ^:unsynchronized-mutable beta-memory
                               ^:unsynchronized-mutable accum-memory
                               ^:unsynchronized-mutable production-memory
                               ^:unsynchronized-mutable activation-map]

  IMemoryReader
  (get-rulebase [memory] rulebase)

  (get-elements [memory node bindings]
    (get (get alpha-memory (:id node) {})
         bindings
         []))

  (get-elements-all [memory node]
    (vals (get alpha-memory (:id node) {})))

  (get-tokens [memory node bindings]
    (get (get beta-memory (:id node) {})
         bindings
         []))

  (get-tokens-all [memory node]
    (vals (get beta-memory (:id node) {})))

  (get-accum-reduced [memory node join-bindings fact-bindings]
    (get-in accum-memory [(:id node) join-bindings fact-bindings] ::no-accum-reduced))

  (get-accum-reduced-all [memory node join-bindings]
    (get
     (get accum-memory (:id node) {})
     join-bindings))

  ;; TODO: rename existing get-accum-reduced-all and use something better here.
  (get-accum-reduced-complete [memory node]
    (for [[join-binding joins] (get accum-memory (:id node) {})
          [fact-binding reduced] joins]
      {:join-bindings join-binding
       :fact-bindings fact-binding
       :result reduced}))

  (get-insertions [memory node token]
    (get
     (get production-memory (:id node) {})
     token
     []))

  (get-activations [memory]
    (apply concat (vals activation-map)))

  ITransientMemory
  (add-elements! [memory node join-bindings elements]
    (let [binding-element-map (get alpha-memory (:id node) {})
          previous-elements (get binding-element-map join-bindings [])]

      (set! alpha-memory
            (assoc! alpha-memory
                    (:id node)
                    (assoc binding-element-map join-bindings (into previous-elements elements))))))

  (remove-elements! [memory node join-bindings elements]
    (let [binding-element-map (get alpha-memory (:id node) {})
          previous-elements (get binding-element-map join-bindings [])
          [removed-elements filtered-elements] (remove-first-of-each elements previous-elements)]

      (set! alpha-memory
            (assoc! alpha-memory
                    (:id node)
                    (assoc binding-element-map join-bindings filtered-elements)))

      ;; Return the removed elements.
      removed-elements))

  (add-tokens! [memory node join-bindings tokens]
    (let [binding-token-map (get beta-memory (:id node) {})
          previous-tokens (get binding-token-map join-bindings [])]

      (set! beta-memory
            (assoc! beta-memory
                    (:id node)
                    (assoc binding-token-map join-bindings (into previous-tokens tokens))))))

  (remove-tokens! [memory node join-bindings tokens]
    (let [binding-token-map (get beta-memory (:id node) {})
          previous-tokens (get binding-token-map join-bindings [])
          [removed-tokens filtered-tokens] (remove-first-of-each tokens previous-tokens)]

      (set! beta-memory
            (assoc! beta-memory
                    (:id node)
                    (assoc binding-token-map join-bindings filtered-tokens)))

      ;; Return the removed tokens.
      removed-tokens))
  
  (add-accum-reduced! [memory node join-bindings accum-result fact-bindings]

    (set! accum-memory
          (assoc! accum-memory
                  (:id node)
                  (assoc-in (get accum-memory (:id node) {})
                            [join-bindings fact-bindings]
                            accum-result))))

  (add-insertions! [memory node token facts]
    (let [token-facts-map (get production-memory (:id node) {})
          previous-facts (get token-facts-map token [])]

      (set! production-memory
            (assoc! production-memory
                    (:id node)
                    (assoc token-facts-map token (into previous-facts facts))))))

  (remove-insertions! [memory node tokens]

    ;; Remove the facts inserted from the given token.
    (let [token-facts-map (get production-memory (:id node) {})
          ;; Get removed tokens for the caller.
          results (doall
                   (flatten
                    (for [token tokens]
                      (get token-facts-map token))))]

      ;; Clear the tokens and update the memory.
      (set! production-memory
            (assoc! production-memory
                    (:id node)
                    (apply dissoc token-facts-map tokens)))

      results))

  #?(:clj
      (add-activations!
        [memory production new-activations]
        (let [activation-group (activation-group-fn production)
              previous (.get activation-map activation-group)]
          (.put activation-map activation-group
                (if previous
                  (into previous new-activations)
                  new-activations))))
      :cljs
      (add-activations!
        [memory production new-activations]
        (let [activation-group (activation-group-fn production)
              previous (get activation-map activation-group)]  
          (set! activation-map
                (assoc activation-map
                  activation-group
                  (if previous
                    (into previous new-activations)
                    new-activations)))))      )

  #?(:clj
      (pop-activation!
        [memory]
        (when (not (.isEmpty activation-map))
          (let [^java.util.Map$Entry entry (.firstEntry activation-map)
                key (.getKey entry)
                value (.getValue entry)
                remaining (rest value)]
            
            (if (empty? remaining)
              (.remove activation-map key)
              (.put activation-map key remaining))
            (first value))))
      :cljs
      (pop-activation!
        [memory]
        (when (not (empty? activation-map))
          (let [[key value] (first activation-map)
                remaining (rest value)]
            
            (set! activation-map
                  (if (empty? remaining)
                    (dissoc activation-map key)
                    (assoc activation-map key remaining)))
            (first value)))))

  #?(:clj
      (next-activation-group
        [memory]
        (when (not (.isEmpty activation-map))
          (let [^java.util.Map$Entry entry (.firstEntry activation-map)]
            (.getKey entry))))
      :cljs
      (next-activation-group
        [memory]
        (let [[key val] (first activation-map)]
          key)))

  #?(:clj
      (remove-activations!
        [memory production to-remove]
        (let [activation-group (activation-group-fn production)]
          (.put activation-map
            activation-group
                (second (remove-first-of-each to-remove
                                              (.get activation-map activation-group))))))
      :cljs
      (remove-activations!
        [memory production to-remove]
        (let [activation-group (activation-group-fn production)]
          (set! activation-map
                (assoc activation-map
                       activation-group
                       (second (remove-first-of-each to-remove
                                                     (get activation-map activation-group))))))))

  #?(:clj
      (clear-activations!
        [memory]
        (.clear activation-map))
      :cljs
      (clear-activations!
        [memory]
        (set! activation-map (sorted-map-by activation-group-sort-fn))))
  
  (to-persistent! [memory]
    (->PersistentLocalMemory rulebase
                             activation-group-sort-fn
                             activation-group-fn
                             (persistent! alpha-memory)
                             (persistent! beta-memory)
                             (persistent! accum-memory)
                             (persistent! production-memory)
                             (into {}
                                   (for [[key val] activation-map]
                                     [key val])))))

(defrecord PersistentLocalMemory [rulebase
                                  activation-group-sort-fn
                                  activation-group-fn
                                  alpha-memory
                                  beta-memory
                                  accum-memory
                                  production-memory
                                  activation-map]
  IMemoryReader
  (get-rulebase [memory] rulebase)


  (get-elements [memory node bindings]
    (get (get alpha-memory (:id node) {})
         bindings
         []))

  (get-elements-all [memory node]
    (flatten (vals (get alpha-memory (:id node) {}))))

  (get-tokens [memory node bindings]
    (get (get beta-memory (:id node) {})
         bindings
         []))

  (get-tokens-all [memory node]
    (flatten (vals (get beta-memory (:id node) {}))))

  (get-accum-reduced [memory node join-bindings fact-bindings]
    ;; nil is a valid previously reduced value that can be found in the map.
    ;; Return ::no-accum-reduced instead of nil when there is no previously
    ;; reduced value in memory.
    (get-in accum-memory [(:id node) join-bindings fact-bindings] ::no-accum-reduced))

  (get-accum-reduced-all [memory node join-bindings]
    (get
     (get accum-memory (:id node) {})
     join-bindings))

  (get-accum-reduced-complete [memory node]
    (for [[join-binding joins] (get accum-memory (:id node) {})
          [fact-binding reduced] joins]
      {:join-bindings join-binding
       :fact-bindings fact-binding
       :result reduced}))

  (get-insertions [memory node token]
    (get
     (get production-memory (:id node) {})
     token
     []))

  (get-activations [memory]
    (apply concat (vals activation-map)))

  IPersistentMemory
  (to-transient [memory]
    #?(:clj
        (TransientLocalMemory. rulebase
                               activation-group-sort-fn
                               activation-group-fn
                               (transient alpha-memory)
                               (transient beta-memory)
                               (transient accum-memory)
                               (transient production-memory)
                               (reduce
                                 (fn [^java.util.TreeMap treemap [activation-group activations]]
                                   (let [previous (.get treemap activation-group)]
                                     (.put treemap activation-group
                                           (if previous
                                             (into previous activations)
                                             activations)))
                                   treemap)
                                 (java.util.TreeMap. ^java.util.Comparator activation-group-sort-fn)
                                 activation-map))
        :cljs
        (let [activation-map (reduce
                               (fn [treemap [activation-group activations]]
                                 (let [previous (get treemap activation-group)]
                                   (assoc treemap activation-group
                                          (if previous
                                            (into previous activations)
                                            activations))))
                               (sorted-map-by activation-group-sort-fn)
                               activation-map)]
          (TransientLocalMemory. rulebase
                                 activation-group-sort-fn
                                 activation-group-fn
                                 (transient alpha-memory)
                                 (transient beta-memory)
                                 (transient accum-memory)
                                 (transient production-memory)
                                 (reduce
                                   (fn [treemap [activation-group activations]]
                                     (let [previous (get treemap activation-group)]
                                       (assoc treemap activation-group
                                              (if previous
                                                (into previous activations)
                                                activations))))
                                   (sorted-map-by activation-group-sort-fn)
                                   activation-map))))))

(defn local-memory
  "Creates an persistent local memory for the given rule base."
  [rulebase activation-group-sort-fn activation-group-fn]

  (->PersistentLocalMemory rulebase
                           activation-group-sort-fn
                           activation-group-fn
                           {}
                           {}
                           {}
                           {}
                           {}))
