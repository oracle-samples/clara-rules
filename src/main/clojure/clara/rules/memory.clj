(ns clara.rules.memory
  "Specification and default implementation of working memory"
  (:require [clojure.core.reducers :as r]
            [clojure.set :as s]))

(defprotocol IPersistentMemory
  (to-transient [memory]))

(defprotocol IMemoryReader
  (get-rulebase [memory])
  (get-elements [memory node bindings])
  (get-tokens [memory node bindings])
  (get-accum-result [memory node join-bindings fact-bindings])
  (get-accum-results [memory node join-bindings])
  (get-insertions [memory node token])
  (is-fired-token [memory node token]))

(defprotocol ITransientMemory
  (add-elements! [memory node join-bindings elements])
  (remove-elements! [memory node elements join-bindings])
  (add-tokens! [memory node join-bindings tokens])
  (remove-tokens! [memory node join-bindings tokens])
  (add-accum-result! [memory node join-bindings accum-result fact-bindings])
  (add-insertions! [memory node token facts])
  (remove-insertions! [memory node token])
  (mark-as-fired! [memory node token])
  (unmark-as-fired! [memory node token])
  (to-persistent! [memory]))

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

  (get-accum-result [memory node join-bindings fact-bindings]
    (get-in content [[:accum-memory (node-to-id node) join-bindings] fact-bindings]))

  (get-accum-results [memory node join-bindings]
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
          filtered-facts (filter (fn [candidate] (not (element-set candidate))) current-facts)]

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
          filtered-tokens (filter #(not (token-set %)) current-tokens)]

      (assoc-mem! key filtered-tokens)
      (s/intersection token-set (set current-tokens))))

  (add-accum-result! [memory node join-bindings accum-result fact-bindings]
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
