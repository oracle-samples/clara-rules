(ns clara.rules.memory
  (:require [clojure.core.reducers :as r]
            [clojure.set :as s]))


(defn- get-node-memory 
  "Helper function to get a node's transient memory, reverting to the persisted
   form and creating it if not present."
  [transient-memory persistent-memory mem-type node]
  (or (get-in transient-memory [mem-type node])
      (let [node-memory (transient (get-in persistent-memory [mem-type node] {}))]
        (assoc! (mem-type transient-memory) node node-memory)
        node-memory)))

(defn- persist-map-of-transients! 
  "Given a transient map with transient values, return a fully persistent form."
  [m]
  (into {} (for [[k v] (persistent! m)] [k (persistent! v)] )))

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

(defrecord TransientLocalMemory [rulebase alpha-memory beta-memory accum-memory production-memory persistent-memory] 
  IMemoryReader 
  (get-rulebase [memory] rulebase)

  (get-elements [memory node bindings]
    (get (get-node-memory memory persistent-memory :alpha-memory node) bindings []))

  (get-tokens [memory node bindings]
    (get (get-node-memory memory persistent-memory :beta-memory node) bindings []))

  (get-accum-result [memory node join-bindings fact-bindings]
    (get-in (get-node-memory memory persistent-memory :accum-memory node) [join-bindings fact-bindings]))

  (get-accum-results [memory node join-bindings]
    (get (get-node-memory memory persistent-memory :accum-memory node) join-bindings {}))

  (get-insertions [memory node token]
    (get (get-node-memory memory persistent-memory :production-memory node) token []))

  (is-fired-token [memory node token]
    (contains? 
     (get (get-node-memory memory persistent-memory :production-memory node) :fired-tokens #{})
     token))
  
  ITransientMemory  
  (add-elements! [memory node join-bindings elements]
    (let [alpha-mem (get-node-memory memory persistent-memory :alpha-memory node)
          current-facts (get alpha-mem join-bindings)]
      (assoc! alpha-mem join-bindings (concat current-facts elements))))

  (remove-elements! [memory node elements join-bindings]
    (let [alpha-mem (get-node-memory memory persistent-memory :alpha-memory node)
          current-facts (get alpha-mem join-bindings)
          element-set (set elements)
          filtered-facts (filter (fn [candidate] (not (element-set candidate))) current-facts)]
      
      ;; Update our memory with the changed facts.
      (assoc! alpha-mem join-bindings filtered-facts)
      ;; If the count of facts changed, we removed something.
      (s/intersection element-set (set current-facts)))) ;; TODO: innefficient..should use sets throughout.

  (add-tokens! [memory node join-bindings tokens]
    (let [beta-mem (get-node-memory memory persistent-memory :beta-memory node)
          current-tokens (get beta-mem join-bindings)]
      (assoc! beta-mem join-bindings (concat current-tokens tokens))))

  (remove-tokens! [memory node join-bindings tokens]
    (let [beta-mem (get-node-memory memory persistent-memory :beta-memory node)
          current-tokens (get beta-mem join-bindings)
          token-set (set tokens)
          filtered-tokens (filter #(not (token-set %)) current-tokens)]
      (assoc! beta-mem join-bindings filtered-tokens)
      ;; If the count of tokens changed, we remove something.
      (s/intersection token-set (set current-tokens))))

  (add-accum-result! [memory node join-bindings accum-result fact-bindings]
    (let [accum-mem (get-node-memory memory persistent-memory :accum-memory node)
          join-binding-map (assoc 
                               (get accum-mem join-bindings {})
                             fact-bindings 
                             accum-result)]
      (assoc! accum-mem join-bindings join-binding-map)))
  
  (add-insertions! [memory node token facts]
    (let [production-mem (get-node-memory memory persistent-memory :production-memory node)
          current-facts (get production-mem token)]
      (assoc! production-mem token (concat current-facts facts))))

  (remove-insertions! [memory node tokens]
    (let [production-mem (get-node-memory memory persistent-memory :production-memory node)]

      (doall ; Avoid laziness since we're clearing a transient memory.
       (flatten
        ;; Remove all facts for each token and return them.
        (for [token tokens
              :let [facts (get production-mem token)]]
          (do
            (assoc! production-mem token [])
            facts))))))

  (mark-as-fired! [memory node token]
    (let [production-mem (get-node-memory memory persistent-memory :production-memory node)
          fired-tokens (get production-mem :fired-tokens #{})]
      (assoc! production-mem :fired-tokens (conj fired-tokens token))))

  (unmark-as-fired! [memory node token]
    (let [production-mem (get-node-memory memory persistent-memory :production-memory node)
          fired-tokens (get production-mem :fired-tokens #{})]
      (assoc! production-mem :fired-tokens (disj fired-tokens token))))
  
  (to-persistent! [memory]

    ;; Create a consistent form of our memory by conj'ing the updated nodes into
    ;; the previous persistent form.
    (->PersistentLocalMemory rulebase 
                             (conj (:alpha-memory persistent-memory) (persist-map-of-transients! alpha-memory)) 
                             (conj (:beta-memory persistent-memory) (persist-map-of-transients! beta-memory)) 
                             (conj (:accum-memory persistent-memory) (persist-map-of-transients! accum-memory)) 
                             (conj (:production-memory persistent-memory) (persist-map-of-transients! production-memory)))))

(defrecord PersistentLocalMemory [rulebase alpha-memory beta-memory accum-memory production-memory]
  IPersistentMemory
  (to-transient [memory] 
    ;; The initial transient local memory has no updates, hence the empty maps.
    (->TransientLocalMemory rulebase 
                            (transient {}) 
                            (transient {}) 
                            (transient {}) 
                            (transient {})
                            memory)))

(defn local-memory 
  "Returns a local, in-process working memory."
  [rulebase]
  (->PersistentLocalMemory rulebase {} {} {} {}))
