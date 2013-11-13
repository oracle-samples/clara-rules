(ns clara.rules.memory
  "Specification and default implementation of working memory"
  (:require [clojure.core.reducers :as r]
            [clojure.set :as s]))

;; Activation record used by get-activations and add-activations! below.
(defrecord Activation [node token])

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

  ;; Add activations.
  (add-activations! [memory node tokens])

  ;; Remove the given activations from the working memory.
  (remove-activations! [memory node tokens])

  ;; Clear all activations from the working memory
  (clear-activations! [memory])

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

(deftype TransientLocalMemory [rulebase ^:unsynchronized-mutable content]
  IMemoryReader
  (get-rulebase [memory] rulebase)

  (get-elements [memory node bindings]
    (get content [:alpha-memory (:id node) bindings] []))

  (get-tokens [memory node bindings]
    (get content [:beta-memory (:id node) bindings] []))

  (get-accum-reduced [memory node join-bindings fact-bindings]
    (get-in content [[:accum-memory (:id node) join-bindings] fact-bindings]))

  (get-accum-reduced-all [memory node join-bindings]
    (get content [:accum-memory (:id node) join-bindings] {}))

  (get-insertions [memory node token]
    (get content [:production-memory (:id node) token] []))

  (get-activations [memory]
    (:activations content))
  
  ITransientMemory  
  (add-elements! [memory node join-bindings elements]
    (let [key [:alpha-memory (:id node) join-bindings]
          current-facts (get content key [])]
      (set! content (assoc! content key (concat current-facts elements)))))

  (remove-elements! [memory node join-bindings elements]
    (let [key [:alpha-memory (:id node) join-bindings]
          current-facts (get content key [])
          element-set (set elements)
          filtered-facts (remove-first-of-each element-set current-facts)]

      ;; Update the memory with the changed facts.
      (set! content (assoc! content key filtered-facts))

      ;; return the removed elements.
      (s/intersection element-set (set current-facts))))

  (add-tokens! [memory node join-bindings tokens]
    (let [key [:beta-memory (:id node) join-bindings]
          current-tokens (get content key [])]
		(set! content (assoc! content key (concat current-tokens tokens)))))

  (remove-tokens! [memory node join-bindings tokens]
    (let [key [:beta-memory (:id node) join-bindings]
          current-tokens (get content key [])
          token-set (set tokens)
          filtered-tokens (remove-first-of-each token-set current-tokens)]

      (set! content (assoc! content key filtered-tokens))
      (s/intersection token-set (set current-tokens))))

  (add-accum-reduced! [memory node join-bindings accum-result fact-bindings]
    (let [key [:accum-memory (:id node) join-bindings]
          updated-bindings (assoc (get content key {})
                             fact-bindings
                             accum-result)]
      (set! content (assoc! content key updated-bindings))))
  
  (add-insertions! [memory node token facts]
    (let [key [:production-memory (:id node) token]
          current-facts (get content key [])]
      (set! content (assoc! content key (concat current-facts facts)))))

  (remove-insertions! [memory node tokens]

    ;; Remove the facts inserted from the given token.
    (let [results (doall
                   (flatten
                    (for [token tokens
                          :let [key [:production-memory (:id node) token]]]
                      (get content key))))]

      ;; Clear the keys.
      (doseq [token tokens
              :let [key [:production-memory (:id node) token]]]
        (set! content (assoc! content  key [])))

      results))

  (add-activations! [memory node tokens]
    (set! content (assoc! content :activations 
                (assoc 
                 (:activations content) 
                  node 
                  (concat tokens (get-in content [:activations node] []))))))

  (remove-activations! [memory node tokens]
    (set! content (assoc! content :activations 
                (assoc 
                 (:activations content) 
                  node 
                  (remove-first-of-each (set tokens) (get-in content [:activations node] []))))))

  (clear-activations! [memory]
    (set! content (assoc! content  :activations {})))

  (to-persistent! [memory]

    (->PersistentLocalMemory rulebase (persistent! content))))

(defrecord PersistentLocalMemory [rulebase content]
  IPersistentMemory
  (to-transient [memory] 
    (TransientLocalMemory. rulebase (transient content))))
