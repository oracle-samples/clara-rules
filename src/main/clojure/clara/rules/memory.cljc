(ns clara.rules.memory
  "This namespace is for internal use and may move in the future.
   Specification and default implementation of working memory"
  (:require [clojure.set :as s]))

;; Activation record used by get-activations and add-activations! below.
(defrecord Activation [node token])

(defprotocol IPersistentMemory
  (to-transient [memory]))

(defprotocol IMemoryReader
  ;; Returns the rulebase associated with the given memory.
  (get-rulebase [memory])

  ;; Returns a function that produces a map of alpha nodes to
  ;; facts that match the type of the node
  (get-alphas-fn [memory])

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

  ;; Returns insertions that occurred at the given node for the given token.
  ;; Returns a sequence of the form
  ;; [facts-inserted-for-one-rule-activation facts-inserted-for-another-rule-activation]
  (get-insertions [memory node token])

  ;; Returns all insertions that occurred in the given node's RHS; this takes the form
  ;; {token [facts-inserted-for-one-rule-activation facts-inserted-for-another-rule-activation]}
  (get-insertions-all [memory node])

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

  ;; Removes the result of a reduced accumulator execution to the given memory and node.
  (remove-accum-reduced! [memory node join-bindings fact-bindings])

  ;; Add a record that a given fact twas inserted at a given node with
  ;; the given support. Used for truth maintenance.
  ;; This should be called at most once per rule activation.
  (add-insertions! [memory node token facts])

  ;; Removes all records of facts that were inserted at the given node
  ;; due to the given token. Used for truth maintenance.
  ;; This function returns a map of each token to the associated facts
  ;; it removed.
  (remove-insertions! [memory node token])

  ;; Add a sequence of activations.
  (add-activations! [memory production activations])

  ;; Pop an activation from the working memory. Returns nil if no
  ;; activations are pending.
  (pop-activation! [memory])

  ;; Returns the group of the next activation, or nil if none are pending.
  (next-activation-group [memory])

  ;; Remove the given activations from the working memory.  This is expected
  ;; to return a tuple of the form [removed-activations unremoved-activations],
  ;; where unremoved-activations is comprised of activations passed to the memory
  ;; for removal but which were not removed because they were not present in the memory's
  ;; store of pending activations.
  (remove-activations! [memory production activations])

  ;; Clear all activations from the working memory
  (clear-activations! [memory])

  ;; Converts the transient memory to persistent form.
  (to-persistent! [memory]))

#?(:clj
   (defn- coll-empty?
     "Returns true if the collection is empty.  Does not call seq due to avoid
      overhead that may cause for non-persistent collection types, e.g.
      java.util.LinkedList, etc."
     [^java.util.Collection coll]
     (or (nil? coll) (.isEmpty coll))))

#?(:clj
   (defn- list-remove!
     "Removes the item, to-remove, from the given list, lst.  If it is found and removed,
      returns true.  Otherwise returns false.  Only removes the first element in the list
      that is equal to to-remove.  Equality is done based on the given eq-pred function.
      If it isn't given, the default is = .  If others are equal, they will not be removed.
      This is similar to java.util.List.remove(Object).  lst is updated in place for performance.
      This implies that the list must support the mutable list interface, namely via the
      java.util.List.listIterator()."
     ([^java.util.List lst to-remove]
      (list-remove! lst to-remove =))
     ([^java.util.List lst to-remove eq-pred]
      (if-not (coll-empty? lst)
        (let [li (.listIterator lst)]
          (loop [x (.next li)]
            (cond
              (eq-pred to-remove x)
              (do
                (.remove li)
                true)

              (.hasNext li)
              (recur (.next li))

              :else
              false)))
        false))))

#?(:clj
   (defn- add-all!
     "Adds all items from source to the destination dest collection
      destructively.  Avoids using Collection.addAll() due to unnecessary
      performance overhead of calling Collection.toArray() on the
      incoming source. Returns dest."
     [^java.util.Collection dest source]
     (doseq [x source]
       (.add dest x))
     dest))

#?(:clj
   (defn- ^java.util.List ->linked-list
     "Creates a new java.util.LinkedList from the coll, but avoids using
      Collection.addAll(Collection) since there is unnecessary overhead 
      in this of calling Collection.toArray() on coll."
     [coll]
     (if (instance? java.util.LinkedList coll)
       coll
       (add-all! (java.util.LinkedList.) coll))))

#?(:clj
   (defn- remove-first-of-each!
     "Remove the first instance of each item in the given remove-seq that
      appears in the collection coll.  coll is updated in place for
      performance.  This implies that the coll must support the mutable
      collection interface method Collection.remove(Object).  Returns a tuple
      of the form [remove-seq-items-removed remove-seq-items-not-removed].
      An optional compare-fn can be given to specify how to compare remove-seq 
      items against items in coll.  The default compare-fn is = .
      For immutable collection removal, use the non-destructive remove-first-of-each
      defined below."
     ([remove-seq ^java.util.List coll]
      (remove-first-of-each! remove-seq coll =))

     ([remove-seq ^java.util.List coll compare-fn]
      ;; Optimization for special case of one item to remove,
      ;; which occurs frequently.
      (if (= 1 (count remove-seq))
        (let [to-remove (first remove-seq)]
          (if (list-remove! coll to-remove compare-fn)
            [remove-seq []]
            [[] remove-seq]))
        
        ;; Otherwise, perform a linear search for items to remove.
        (loop [to-remove (first remove-seq)
               remove-seq (next remove-seq)
               removed (transient [])
               not-removed (transient [])]
          (if to-remove
            (let [found? (list-remove! coll to-remove compare-fn)
                  removed (if found?
                            (conj! removed to-remove)
                            removed)
                  not-removed (if found?
                                not-removed
                                (conj! not-removed to-remove))]
              (recur (first remove-seq)
                     (next remove-seq)
                     removed
                     not-removed))
            ;; If this is expensive, using a mutable collection maybe good to
            ;; consider here in a future optimization.
            [(persistent! removed) (persistent! not-removed)]))))))

(defn remove-first-of-each
  "Remove the first instance of each item in the given remove-seq that
   appears in the collection.  This also tracks which items were found
   and removed.  Returns a tuple of the form:
   [items-removed coll-with-items-removed items-not-removed]
   This function does so eagerly since
   the working memories with large numbers of insertions and retractions
   can cause lazy sequences to become deeply nested."
  [remove-seq coll]
  (cond

    ;; There is nothing to remove.
    (empty? remove-seq) [[] coll]

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

              [(persistent! items-removed) (persistent! result) remove-seq]))))

#?(:clj
   (defn fast-token-compare [compare-fn token other-token]
     ;; Fastest path is if the two tokens are truly identical.
     (or (identical? token other-token)
         ;; Assumption is that both arguments given are tokens already.
         (and (let [bindings (:bindings token)
                    other-bindings (:bindings other-token)]
                ;; Calling `count` on these Clojure maps shows up as a bottleneck
                ;; even with clojure.lang.IPersistentMap being clojure.lang.Counted unfortunately.
                (and (= (.size ^java.util.Map bindings)
                        (.size ^java.util.Map other-bindings))
                     ;; `every?` is too slow for a performance critical place like this.  It
                     ;; calls `seq` too many times on the underlying maps.  Instead `seq` one
                     ;; time and keep using that same seq.
                     ;; Also avoiding Clojure destructuring since even that is not as fast
                     ;; pre-1.9.0.
                     (if-let [^clojure.lang.ISeq entries (.seq ^clojure.lang.Seqable bindings)]
                       ;; Type hint to Indexed vs MapEntry just because MapEntry seems to be a
                       ;; less stable impl detail to rely on.
                       (loop [^clojure.lang.Indexed entry (.first entries)
                              entries (.next entries)]
                         (let [k (some-> entry (.nth 0))
                               v (some-> entry (.nth 1))]
                           (if (and k
                                    ;; other-bindings will always be persistent map so invoke
                                    ;; it directly.  It is faster than `get`.
                                    (compare-fn v (other-bindings k)))
                             (recur (some-> entries .first)
                                    (some-> entries .next))
                             ;; If there is no k left, then every entry matched.  If there is a k,
                             ;; that means the comparison failed, so the maps aren't equal.
                             (not k))))
                       ;; Empty bindings on both sides.
                       true))) 

              ;; Check the :matches on each token.  :matches need to be in the same order on both
              ;; tokens to be considered the same.
              (let [^clojure.lang.Indexed matches (:matches token)
                    ^clojure.lang.Indexed other-matches (:matches other-token)
                    count-matches (.size ^java.util.List matches)]
                (and (= count-matches
                        (.size ^java.util.List other-matches))
                     (loop [i 0]
                       (cond
                         (= i count-matches)
                         true

                         ;; Compare node-id's first.  Fallback to comparing facts.  This will
                         ;; most very likely be the most expensive part to execute.
                         (let [^clojure.lang.Indexed m (.nth matches i)
                               ^clojure.lang.Indexed om (.nth other-matches i)]
                           ;; A token :matches tuple is of the form [fact node-id].
                           (and (= (.nth m 1) (.nth om 1))
                                (compare-fn (.nth m 0) (.nth om 0))))
                         (recur (inc i))

                         :else
                         false))))))))

#?(:clj
   (defprotocol IdentityComparable
     (using-token-identity! [this bool])))

#?(:clj
   (deftype RuleOrderedActivation [node-id
                                   token
                                   activation
                                   rule-load-order
                                   ^:unsynchronized-mutable use-token-identity?]
     IdentityComparable
     ;; NOTE!  This should never be called on a RuleOrderedActivation instance that has been stored
     ;; somewhere in local memory because it could cause interference across threads that have
     ;; multiple versions of local memories that are sharing some of their state.  This is only intended
     ;; to be called by ephemeral, only-local references to RuleOrderedActivation instances used to
     ;; search for activations to remove from memory when performing `remove-activations!` operations.
     ;; The reason this mutable state exists at all is to "flip" a single instance of a RuleOrderedActivation
     ;; from identity to value equality based comparable when doing the "two-pass" removal search operation
     ;; of `remove-activations!`.  This avoids having to create different instances for each pass.
     (using-token-identity! [this bool]
       (set! use-token-identity? bool)
       this)
     Object
     ;; Two RuleOrderedActivation instances should be equal if and only if their
     ;; activation is equal.  Note that if the node of two activations is the same,
     ;; their rule-load-order must be the same.  Using a deftype wrapper allows us to
     ;; use Clojure equality to determine this while placing the wrapper in a Java data
     ;; structure that uses Java equality; the Java equality check will simply end up calling
     ;; Clojure equality checks.
     (equals [this other]
       ;; Note that the .equals method is only called by PriorityQueue.remove, and the object provided
       ;; to the .remove method will never be identical to any object in the queue.  A short-circuiting
       ;; check for reference equality would therefore be pointless here because it would never be true.
       (boolean
        (when (instance? RuleOrderedActivation other)
          (let [^RuleOrderedActivation other other]
            (and
             ;; If the node-id of two nodes is equal then we can assume that the nodes are equal.
             (= node-id
                (.node-id other))

             ;; We check with identity based semantics on the other when the use-token-identity? field
             ;; indicates to do so.
             (if (or use-token-identity? (.-use-token-identity? other))
               (fast-token-compare identical? token (.-token other))
               (fast-token-compare = token (.-token other))))))))))

#?(:clj
   (defn- ->rule-ordered-activation
     "Take an activation from the engine and wrap it in a map that includes information
      on the rule load order.  In Clojure, as opposed to ClojureScript, each activation should
      be wrapped in this way exactly once (that is, the value of the :activation key should
      be an activation from the engine.)"
     ([activation]
      (->rule-ordered-activation activation false))
     ([activation use-token-identity?]
      (let [node (:node activation)]
        (RuleOrderedActivation. (:id node)
                                (:token activation)
                                activation
                                (or (-> node
                                        :production
                                        meta
                                        :clara.rules.compiler/rule-load-order)
                                    0)
                                use-token-identity?)))))

#?(:clj
   (defn- queue-activations!
     "Add activations to a queue. The wrap-rule-order? option should be true
      unless the activations in question have previously been wrapped."
     ([^java.util.Queue pq activations]
      (queue-activations! pq activations true))
     ([^java.util.Queue pq activations wrap-rule-order?]
      (if wrap-rule-order?
        (doseq [act activations]
          (.add pq (->rule-ordered-activation act)))
        (doseq [act activations]
          (.add pq act)))
      pq)))

#?(:clj
   (defn- ->activation-priority-queue
     "Given activations, create a priority queue based on rule ordering.
      The activations should be wrapped by using the wrap-rule-order? option
      if they have not been wrapped already."
     ([activations]
      (->activation-priority-queue activations true))
     ([activations wrap-rule-order?]
      (let [init-cnt (count activations)
            ;; Note that 11 is the default initial value; there is no constructor
            ;; for PriorityQueue that takes a custom comparator and does not require
            ;; an initial size to be passed.
            pq (java.util.PriorityQueue. (if (pos? init-cnt) init-cnt 11)
                                         (fn [^RuleOrderedActivation x
                                              ^RuleOrderedActivation y]
                                           (compare (.rule-load-order x)
                                                    (.rule-load-order y))))]
        (queue-activations! pq activations wrap-rule-order?)))))

(declare ->PersistentLocalMemory)

;;; Transient local memory implementation. Typically only persistent memory will be visible externally.

(deftype TransientLocalMemory [rulebase
                               activation-group-sort-fn
                               activation-group-fn
                               alphas-fn
                               ^:unsynchronized-mutable alpha-memory
                               ^:unsynchronized-mutable beta-memory
                               ^:unsynchronized-mutable accum-memory
                               ^:unsynchronized-mutable production-memory
                               ^:unsynchronized-mutable #?(:clj ^java.util.NavigableMap activation-map :cljs activation-map)]

  IMemoryReader
  (get-rulebase [memory] rulebase)

  (get-alphas-fn [memory] alphas-fn)

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

  (get-insertions-all [memory node]
    (get production-memory (:id node) {}))

  (get-activations [memory]
    (into []
          (comp cat
                (map (fn [^RuleOrderedActivation a]
                       (.activation a))))
          (vals activation-map)))

  ITransientMemory
  #?(:clj
      (add-elements! [memory node join-bindings elements]
                     (let [binding-element-map (get alpha-memory (:id node) {})
                           previous-elements (get binding-element-map join-bindings)]

                       (cond
                         ;; When changing existing persistent collections, just add on
                         ;; the new elements.
                         (coll? previous-elements)
                         (set! alpha-memory
                               (assoc! alpha-memory
                                       (:id node)
                                       (assoc binding-element-map
                                              join-bindings
                                              (into previous-elements elements))))

                         ;; Already mutable, so update-in-place.
                         previous-elements
                         (add-all! previous-elements elements)

                         ;; No previous.  We can just leave it persistent if it is
                         ;; until we actually need to modify anything.  This avoids
                         ;; unnecessary copying.
                         elements
                         (set! alpha-memory
                               (assoc! alpha-memory
                                       (:id node)
                                       (assoc binding-element-map
                                              join-bindings
                                              elements))))))
      :cljs
      (add-elements! [memory node join-bindings elements]
                     (let [binding-element-map (get alpha-memory (:id node) {})
                           previous-elements (get binding-element-map join-bindings [])]

                       (set! alpha-memory
                             (assoc! alpha-memory
                                     (:id node)
                                     (assoc binding-element-map join-bindings (into previous-elements elements)))))))

  (remove-elements! [memory node join-bindings elements]
    #?(:clj
       ;; Do nothing when no elements to remove.
       (when-not (coll-empty? elements)
         (let [binding-element-map (get alpha-memory (:id node) {})
               previous-elements (get binding-element-map join-bindings)]
           (cond
             ;; Do nothing when no previous elements to remove from.
             (coll-empty? previous-elements)
             []

             ;; Convert persistent collection to a mutable one prior to calling remove-first-of-each!
             ;; alpha-memory needs to be updated this time since there is now going to be a mutable
             ;; collection associated in this memory location instead.
             (coll? previous-elements)
             (let [remaining-elements (->linked-list previous-elements)
                   removed-elements (first (remove-first-of-each! elements remaining-elements))]
               (set! alpha-memory
                     (assoc! alpha-memory
                             (:id node)
                             (assoc binding-element-map
                                    join-bindings
                                    remaining-elements)))
               removed-elements)

             ;; Already mutable, so we do not need to re-associate to alpha-memory.
             previous-elements
             (first (remove-first-of-each! elements previous-elements)))))
       :cljs
       (let [binding-element-map (get alpha-memory (:id node) {})
             previous-elements (get binding-element-map join-bindings [])
             [removed-elements filtered-elements] (remove-first-of-each elements previous-elements)]

         (set! alpha-memory
               (assoc! alpha-memory
                       (:id node)
                       (assoc binding-element-map join-bindings filtered-elements)))

         ;; Return the removed elements.
         removed-elements)))

  (add-tokens! [memory node join-bindings tokens]
    #?(:clj
       (let [binding-token-map (get beta-memory (:id node) {})
             previous-tokens (get binding-token-map join-bindings)]
         ;; The reasoning here is the same as in add-elements! impl above.
         (cond
           (coll? previous-tokens)
           (set! beta-memory
                 (assoc! beta-memory
                         (:id node)
                         (assoc binding-token-map
                                join-bindings
                                (into previous-tokens tokens))))
           
           previous-tokens
           (add-all! previous-tokens tokens)

           tokens
           (set! beta-memory
                 (assoc! beta-memory
                         (:id node)
                         (assoc binding-token-map
                                join-bindings
                                tokens)))))
       :cljs
       (let [binding-token-map (get beta-memory (:id node) {})
             previous-tokens (get binding-token-map join-bindings [])]

         (set! beta-memory
               (assoc! beta-memory
                       (:id node)
                       (assoc binding-token-map join-bindings (into previous-tokens tokens)))))))

  (remove-tokens! [memory node join-bindings tokens]
    #?(:clj
       ;; The reasoning here is the same as remove-elements!
       (when-not (coll-empty? tokens)
         (let [binding-token-map (get beta-memory (:id node) {})
               previous-tokens (get binding-token-map join-bindings)]
           (if
             (coll-empty? previous-tokens)
             []

             (let [;; Attempt to remove tokens using the faster indentity-based equality first since
                   ;; most of the time this is all we need and it can be much faster.  Any token that
                   ;; wasn't removed via identity, has to be "retried" with normal value-based
                   ;; equality though since those semantics are supported within the engine.  This
                   ;; slower path should be rare for any heavy retraction flows - such as those that come
                   ;; via truth maintenance.
                   two-pass-remove! (fn [remaining-tokens tokens]
                                      (let [[removed-tokens not-removed-tokens]
                                            (remove-first-of-each! tokens
                                                                   remaining-tokens
                                                                   (fn [t1 t2]
                                                                     (fast-token-compare identical? t1 t2)))]

                                        (if-let [other-removed (and (seq not-removed-tokens)
                                                                    (-> not-removed-tokens
                                                                        (remove-first-of-each! remaining-tokens
                                                                                               (fn [t1 t2]
                                                                                                 (fast-token-compare = t1 t2)))
                                                                        first
                                                                        seq))]
                                          (into removed-tokens other-removed)
                                          removed-tokens)))]
               (cond
                 (coll? previous-tokens)
                 (let [remaining-tokens (->linked-list previous-tokens)
                       removed-tokens (two-pass-remove! remaining-tokens tokens)]
                   (set! beta-memory
                         (assoc! beta-memory
                                 (:id node)
                                 (assoc binding-token-map
                                        join-bindings
                                        remaining-tokens)))
                   removed-tokens)

                 previous-tokens
                 (two-pass-remove! previous-tokens tokens))))))
       :cljs
       (let [binding-token-map (get beta-memory (:id node) {})
             previous-tokens (get binding-token-map join-bindings [])
             [removed-tokens filtered-tokens] (remove-first-of-each tokens previous-tokens)]

         (set! beta-memory
               (assoc! beta-memory
                       (:id node)
                       (assoc binding-token-map join-bindings filtered-tokens)))

         ;; Return the removed tokens.
         removed-tokens)))

  (add-accum-reduced! [memory node join-bindings accum-result fact-bindings]

    (set! accum-memory
          (assoc! accum-memory
                  (:id node)
                  (assoc-in (get accum-memory (:id node) {})
                            [join-bindings fact-bindings]
                            accum-result))))

  (remove-accum-reduced! [memory node join-bindings fact-bindings]
    (let [node-id (:id node)
          node-id-mem (get accum-memory node-id {})
          join-mem (dissoc (get node-id-mem join-bindings) fact-bindings)
          node-id-mem (if (empty? join-mem)
                        (dissoc node-id-mem join-bindings)
                        (assoc node-id-mem join-bindings join-mem))]
      (set! accum-memory
            (if (empty? node-id-mem)
              (dissoc! accum-memory
                       node-id)
              (assoc! accum-memory
                      node-id
                      node-id-mem)))))

  ;; The value under each token in the map should be a sequence
  ;; of sequences of facts, with each inner sequence coming from a single
  ;; rule activation.
  (add-insertions! [memory node token facts]
    (let [token-facts-map (get production-memory (:id node) {})]
      (set! production-memory
            (assoc! production-memory
                    (:id node)
                    (update token-facts-map token conj facts)))))

  (remove-insertions! [memory node tokens]

    ;; Remove the facts inserted from the given token.
    (let [token-facts-map (get production-memory (:id node) {})
          ;; Get removed tokens for the caller.
          [results
           new-token-facts-map]

          (loop [results (transient {})
                 token-map (transient token-facts-map)
                 to-remove tokens]
            (if-let [head-token (first to-remove)]
              ;; Don't use contains? due to http://dev.clojure.org/jira/browse/CLJ-700
              (if-let [token-insertions (get token-map head-token)]
                (let [;; There is no particular significance in removing the
                      ;; first group; we just need to remove exactly one.
                      [removed-facts & remaining-facts] token-insertions
                      removed-insertion-map (if (not-empty remaining-facts)
                                              (assoc! token-map head-token remaining-facts)
                                              (dissoc! token-map head-token))
                      prev-token-result (get results head-token [])]
                  (recur (assoc! results head-token (into prev-token-result removed-facts))
                         removed-insertion-map
                         (rest to-remove)))
                ;; If the token isn't present in the insertions just try the next one.
                (recur results token-map (rest to-remove)))
              [(persistent! results)
               (persistent! token-map)]))]

      ;; Clear the tokens and update the memory.
      (set! production-memory
            (if (not-empty new-token-facts-map)
              (assoc! production-memory
                      (:id node)
                      new-token-facts-map)
              (dissoc! production-memory (:id node))))
      results))

  #?(:clj
      (add-activations!
        [memory production new-activations]
        (let [activation-group (activation-group-fn production)
              previous (.get activation-map activation-group)]
          ;; The reasoning here is the same as in add-elements! impl above.
          (cond
            previous
            (queue-activations! previous new-activations)

            (not (coll-empty? new-activations))
            (.put activation-map
                  activation-group
                  (->activation-priority-queue new-activations)))))
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
                    new-activations))))))

  #?(:clj
      (pop-activation!
        [memory]
        (when (not (.isEmpty activation-map))
          (let [entry (.firstEntry activation-map)
                key (.getKey entry)
                ^java.util.Queue value (.getValue entry)

                ;; An empty value is illegal and should be removed by an action
                ;; that creates one (e.g. a remove-activations! call).
                ^RuleOrderedActivation activation (.remove value)]

            ;; This activation group is empty now, so remove it from
            ;; the map entirely.
            (when (.isEmpty value)
              (.remove activation-map key))

            ;; Return the selected activation.
            (.activation activation))))

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
          (let [entry (.firstEntry activation-map)]
            (.getKey entry))))
      :cljs
      (next-activation-group
        [memory]
        (let [[key val] (first activation-map)]
          key)))

  (remove-activations! [memory production to-remove]
    #?(:clj
       ;; The reasoning here is the same as remove-elements!
       (when-not (coll-empty? to-remove)
         (let [activation-group (activation-group-fn production)
               ^java.util.Queue activations (.get activation-map activation-group)
               removed-activations (java.util.LinkedList.)
               unremoved-activations (java.util.LinkedList.)]

           (if (coll-empty? activations)
             ;; If there are no activations present under the group
             ;; then we can't remove any.
             [[] to-remove]
             ;; Remove as many activations by identity as possible first.
             (let [not-removed (loop [to-remove-item (first to-remove)
                                      to-remove (next to-remove)
                                      not-removed (transient [])]
                                 (if to-remove-item
                                   (let [^RuleOrderedActivation act (->rule-ordered-activation to-remove-item true)]
                                     (if (.remove activations act)
                                       (do
                                         (.add removed-activations (.activation act))
                                         ;; The activations that are not removed in the identity checking sweep
                                         ;; are a superset of the activations that are removed after the equality
                                         ;; sweep finishes so we can just create the list of unremoved activations
                                         ;; during that sweep.
                                         (recur (first to-remove) (next to-remove) not-removed))
                                       (recur (first to-remove) (next to-remove) (conj! not-removed act))))
                                   (persistent! not-removed)))]

               ;; There may still be activations not removed since the removal may be based on value-based
               ;; equality semantics.  Retractions in the engine do not require that identical object references
               ;; are given to remove object values that are equal.
               (doseq [^RuleOrderedActivation act not-removed]
                 (if (.remove activations (using-token-identity! act false))
                   (.add removed-activations (.activation act))
                   (.add unremoved-activations (.activation act))))

               (when (coll-empty? activations)
                 (.remove activation-map activation-group))

               [(java.util.Collections/unmodifiableList removed-activations)
                (java.util.Collections/unmodifiableList unremoved-activations)]))))
       :cljs
       (let [activation-group (activation-group-fn production)
             current-activations (get activation-map activation-group)
             [removed-activations remaining-activations unremoved-activations]
             (remove-first-of-each
              to-remove
              current-activations)]
         (set! activation-map (assoc activation-map
                                     activation-group
                                     remaining-activations))
         [removed-activations unremoved-activations])))

  #?(:clj
      (clear-activations!
        [memory]
        (.clear activation-map))
      :cljs
      (clear-activations!
        [memory]
        (set! activation-map (sorted-map-by activation-group-sort-fn))))

  (to-persistent! [memory]
    #?(:clj
       ;; Be sure to remove all transients and internal mutable
       ;; collections used in memory.  Convert any collection that is
       ;; not already a Clojure persistent collection.
       (let [->persistent-coll #(if (coll? %)
                                  %
                                  (seq %))
             update-vals (fn [m update-fn]
                           (->> m
                                (reduce-kv (fn [m k v]
                                             (assoc! m k (update-fn v)))
                                           (transient m))
                                persistent!))
             persistent-vals #(update-vals % ->persistent-coll)]
         (->PersistentLocalMemory rulebase
                                  activation-group-sort-fn
                                  activation-group-fn
                                  alphas-fn
                                  (update-vals (persistent! alpha-memory) persistent-vals)
                                  (update-vals (persistent! beta-memory) persistent-vals)
                                  (persistent! accum-memory)
                                  (persistent! production-memory)
                                  (into {}
                                        (map (juxt key (comp ->persistent-coll val)))
                                        activation-map)))
       :cljs
       (->PersistentLocalMemory rulebase
                                activation-group-sort-fn
                                activation-group-fn
                                alphas-fn
                                (persistent! alpha-memory)
                                (persistent! beta-memory)
                                (persistent! accum-memory)
                                (persistent! production-memory)
                                (into {}
                                      (for [[key val] activation-map]
                                        [key val]))))))

(defrecord PersistentLocalMemory [rulebase
                                  activation-group-sort-fn
                                  activation-group-fn
                                  alphas-fn
                                  alpha-memory
                                  beta-memory
                                  accum-memory
                                  production-memory
                                  activation-map]
  IMemoryReader
  (get-rulebase [memory] rulebase)

  (get-alphas-fn [memory] alphas-fn)

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

  (get-insertions-all [memory node]
    (get production-memory (:id node) {}))

  (get-activations [memory]
    #?(:clj
       (into []
             (comp cat
                   (map (fn [^RuleOrderedActivation a]
                          (.activation a))))
             (vals activation-map))
       
       :cljs
       (apply concat (vals activation-map))))

  IPersistentMemory
  (to-transient [memory]
    #?(:clj
       (TransientLocalMemory. rulebase
                              activation-group-sort-fn
                              activation-group-fn
                              alphas-fn
                              (transient alpha-memory)
                              (transient beta-memory)
                              (transient accum-memory)
                              (transient production-memory)
                              (let [treemap (java.util.TreeMap. ^java.util.Comparator activation-group-sort-fn)]
                                (doseq [[activation-group activations] activation-map]
                                  (.put treemap
                                        activation-group
                                        (->activation-priority-queue activations false)))
                                treemap))
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
                                 alphas-fn
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
  [rulebase activation-group-sort-fn activation-group-fn alphas-fn]

  (->PersistentLocalMemory rulebase
                           activation-group-sort-fn
                           activation-group-fn
                           alphas-fn
                           {}
                           {}
                           {}
                           {}
                           {}))
