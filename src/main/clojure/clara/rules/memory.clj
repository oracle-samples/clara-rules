(ns clara.rules.memory
  "Specification and default implementation of working memory"
  (:require [clojure.core.reducers :as r]
            [clojure.set :as s]))

(defprotocol IPersistentMemory
  (to-transient [memory]))

(defprotocol IMemoryReader
  ;; Returns the rulebase associated with the given memor.
  (get-rulebase [memory])

  ;; Returns the elements assoicated with the given node.
  (get-elements [memory node bindings])

  ;; Returns the tokens associated with the given node.
  (get-tokens [memory node bindings])

  ;; Returns the reduced form of objects processed by an accumulator node
  ;; for facts that match the given bindings.
  (get-accum-reduced [memory node join-bindings fact-bindings])

  ;; Returns all reduced results for the given node that matches
  ;; the given join bindings, independent of the individual fact-bindings
  ;; created by the accumulator's condition.
  (get-accum-reduced-all [memory node join-bindings])

  ;; Returns insertions that occurred at the given node.
  (get-insertions [memory node token])

  ;; Returns true if the token has resulted in a rule execution
  ;; at the given node.
  (is-fired-token [memory node token]))

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

  ;; Mark the given node (a production node) was fired
  ;; due to the given token.
  (mark-as-fired! [memory node token])

  ;; Unmark a node as fired due to the given token.
  (unmark-as-fired! [memory node token])

  ;; Converts the transient memory to persistent form.
  (to-persistent! [memory]))


(defn- remove-first-of-each 
  "Remove the first instance of each item in the given set that 
   appears in the collection."
  [set coll]
  (lazy-seq 
   (when-let [s (seq coll)]
     (let [f (first s)
           r (rest s)]
       (if (set f)
         (if (empty? set)
           r ; No more items to remove, so return the rest of the list.
           (remove-first-of-each (disj set f) r)) ; More items to remove, so recur with a smaller set.

         (cons f (remove-first-of-each set r))))))) ; We did not match this item, so simply recur.

(declare ->PersistentLocalMemory)

;;; Transient local memory implementation. Typically only persistent memory will be visible externally.

;;; Internal helper macro to manage our transient memory.
;;; We must update the content on each call to correctly use the underlying transient map.
(defmacro assoc-mem! [k v]
  `(set! ~'content (assoc! ~'content ~k ~v)))

(deftype TransientLocalMemory [rulebase node-to-id ^:unsynchronized-mutable content]
  IMemoryReader
  (get-rulebase [memory] rulebase)

  (get-elements [memory node bindings]
    (get content [:alpha-memory (node-to-id node) bindings] []))

  (get-tokens [memory node bindings]
    (get content [:beta-memory (node-to-id node) bindings] []))

  (get-accum-reduced [memory node join-bindings fact-bindings]
    (get-in content [[:accum-memory (node-to-id node) join-bindings] fact-bindings]))

  (get-accum-reduced-all [memory node join-bindings]
    (get content [:accum-memory (node-to-id node) join-bindings] {}))

  (get-insertions [memory node token]
    (get content [:production-memory (node-to-id node) token] []))

  (is-fired-token [memory node token]
    (contains?
     (get content [:production-memory (node-to-id node) :fired-tokens] #{})
     token))
  
  ITransientMemory  
  (add-elements! [memory node join-bindings elements]
    (let [key [:alpha-memory (node-to-id node) join-bindings]
          current-facts (get content key [])]
      (assoc-mem! key (concat current-facts elements)) ))

  (remove-elements! [memory node join-bindings elements]
    (let [key [:alpha-memory (node-to-id node) join-bindings]
          current-facts (get content key [])
          element-set (set elements)
          filtered-facts (remove-first-of-each element-set current-facts)]

      ;; Update the memory with the changed facts.
      (assoc-mem! key filtered-facts)

      ;; return the removed elements.
      (s/intersection element-set (set current-facts))))

  (add-tokens! [memory node join-bindings tokens]
    (let [key [:beta-memory (node-to-id node) join-bindings]
          current-tokens (get content key [])]
      (assoc-mem! key (concat current-tokens tokens))))

  (remove-tokens! [memory node join-bindings tokens]
    (let [key [:beta-memory (node-to-id node) join-bindings]
          current-tokens (get content key [])
          token-set (set tokens)
          filtered-tokens (remove-first-of-each token-set current-tokens)]

      (assoc-mem! key filtered-tokens)
      (s/intersection token-set (set current-tokens))))

  (add-accum-reduced! [memory node join-bindings accum-result fact-bindings]
    (let [key [:accum-memory (node-to-id node) join-bindings]
          updated-bindings (assoc (get content key {})
                             fact-bindings
                             accum-result)]
      (assoc-mem! key updated-bindings)))
  
  (add-insertions! [memory node token facts]
    (let [key [:production-memory (node-to-id node) token]
          current-facts (get content key [])]
      (assoc-mem! key (concat current-facts facts))))

  (remove-insertions! [memory node tokens]

    ;; Remove the facts inserted from the given token.
    (let [results (doall
                   (flatten
                    (for [token tokens
                          :let [key [:production-memory (node-to-id node) token]]]
                      (get content key))))]

      ;; Clear the keys.
      (doseq [token tokens
              :let [key [:production-memory (node-to-id node) token]]]
        (assoc-mem! key []))

      results))

  (mark-as-fired! [memory node token]
    (let [key [:production-memory (node-to-id node) :fired-tokens]
          fired-tokens (get content key #{})]
      (assoc-mem! key (conj fired-tokens token))))

  (unmark-as-fired! [memory node token]
    (let [key [:production-memory (node-to-id node) :fired-tokens]
          fired-tokens (get content key #{})]
      (assoc-mem! key (disj fired-tokens token))))

  (to-persistent! [memory]

    (->PersistentLocalMemory rulebase (persistent! content))))

(defrecord PersistentLocalMemory [rulebase content]
  IPersistentMemory
  (to-transient [memory] 
    (TransientLocalMemory. rulebase (:node-to-id rulebase) (transient content))))
