(ns clara.memory
  (:require [clojure.core.reducers :as r]
            [clojure.set :as s]))


(defn- get-node-memory [memory node]
  (or (get memory node)
      (let [node-memory (transient {})]
        (assoc! memory node node-memory)
        node-memory)))

(defprotocol IPersistentMemory
  (to-transient [memory]))

(defprotocol IMemoryReader
  (get-elements [memory node bindings])
  (get-tokens [memory node bindings])
  (get-accum-result [memory node join-bindings fact-bindings]) ; TODO: Is the join-bindings dimension necessary here?
  (get-accum-results [memory node join-bindings]))

(defprotocol ITransientMemory
  (add-elements! [memory node join-bindings elements])
  (remove-elements! [memory node elements join-bindings])
  (add-tokens! [memory node join-bindings tokens])
  (remove-tokens! [memory node join-bindings tokens])
  (add-accum-result! [memory node join-bindings accum-result fact-bindings])
  (to-persistent! [memory]))

(declare ->PersistentLocalMemory)

(deftype TransientLocalMemory [alpha-memory beta-memory accum-memory] 
  IMemoryReader 
  (get-elements [memory node bindings]
    (get (get-node-memory alpha-memory node) bindings []))

  (get-tokens [memory node bindings]
    (get (get-node-memory beta-memory node) bindings []))

  (get-accum-result [memory node join-bindings fact-bindings]
    (get-in (get-node-memory accum-memory node) [join-bindings fact-bindings]))

  (get-accum-results [memory node join-bindings]
    (get (get-node-memory accum-memory node) join-bindings {}))
  
  ITransientMemory  
  (add-elements! [memory node join-bindings elements]
    (let [alpha-mem (get-node-memory alpha-memory node)
          current-facts (get alpha-mem join-bindings)]
      (assoc! alpha-mem join-bindings (concat current-facts elements))))

  (remove-elements! [memory node elements join-bindings]
    (let [alpha-mem (get-node-memory alpha-memory node)
          current-facts (get alpha-mem join-bindings)
          element-set (set elements)
          filtered-facts (filter (fn [candidate] (not (element-set candidate))) current-facts)]
      
      ;; Update our memory with the changed facts.
      (assoc! alpha-mem join-bindings filtered-facts)
      ;; If the count of facts changed, we removed something.
      (s/intersection element-set (set current-facts)))) ;; TODO: innefficient..should use sets throughout.

  (add-tokens! [memory node join-bindings tokens]
    (let [beta-mem (get-node-memory beta-memory node)
          current-tokens (get beta-mem join-bindings)]
      (assoc! beta-mem join-bindings (concat current-tokens tokens))))

  (remove-tokens! [memory node join-bindings tokens]
    (let [beta-mem (get-node-memory beta-memory node)
          current-tokens (get beta-mem join-bindings)
          token-set (set tokens)
          filtered-tokens (filter #(not (token-set %)) current-tokens)]
      (assoc! beta-mem join-bindings filtered-tokens)
      ;; If the count of tokens changed, we remove something.
      (s/intersection token-set (set current-tokens))))

  (add-accum-result! [memory node join-bindings accum-result fact-bindings]
    (let [accum-mem (get-node-memory accum-memory node)
          join-binding-map (assoc 
                               (get accum-mem join-bindings {})
                             fact-bindings 
                             accum-result)]
      (assoc! accum-mem join-bindings join-binding-map)))

  (to-persistent! [memory]
    (->PersistentLocalMemory (persistent! alpha-memory) (persistent! beta-memory) (persistent! accum-memory))))

(defrecord PersistentLocalMemory [alpha-memory beta-memory accum-memory]
  IPersistentMemory
  (to-transient [_] (->TransientLocalMemory (transient alpha-memory) (transient beta-memory) (transient accum-memory))))

(defn local-memory 
  "Returns a local, in-process working memory."
  []
  (->PersistentLocalMemory {} {} {}))
