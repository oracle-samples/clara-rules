(ns clara.rules.durability
  "Experimental namespace. This may change non-passively without warning.

   Support for persisting Clara sessions to an external store. This will have two models: obtaining the full
   state of a session, and obtaining a \"diff\" of changes from a previous state.

   See the session-state function to retrieve the full state of a session as a data structure that can
   be easily serialized via EDN or Fressian. Sessions can then be recovered with the restore-session-state function.

   TODO: diff support is pending -- functions for obtaining a diff of state from a previous point, allowing for a write-ahead log."
  (:require [clara.rules :refer :all]
            [clara.rules.listener :as l]
            [clara.rules.engine :as eng]
            [clara.rules.memory :as mem]
            [clojure.set :as set]
            [schema.core :as s]
            [schema.macros :as sm])

  (:import [clara.rules.engine JoinNode RootJoinNode Token AccumulateNode ProductionNode]))


;; A schema representing a minimal representation of a rule session's state.
;; This allows for efficient storage, particularly when serialized with Fressian or a similar format
;; to eliminate redundnacy.
(def session-state-schema
  {
   ;; Map of matching facts to the number of them that matched.
   :fact-counts {s/Any s/Int}

   ;; Map of node IDs to tokens indicating a pending rule activation.
   :activations {s/Int [Token]}

   ;; Map of accumulator node IDs to accumulated results.
   :accum-results {s/Int [{:join-bindings {s/Keyword s/Any} :fact-bindings {s/Keyword s/Any} :result s/Any}]}

   ;; Map associating node ids and tokens with a vector of facts they had inserted. This is used
   ;; to track truth maintenance, so if a given token is retracted, we can also retract the inferred items.
   :insertions {[(s/one s/Int "node-id") (s/one Token "token")] [s/Any]}})

(sm/defn session-state :- session-state-schema
  " Returns the state of a session as an EDN- or Fressian-serializable data structure. The returned
   structure contains only the minimal data necessary to reconstruct the session via the restore-session-state
   function below."
  [session]
  (let [{:keys [rulebase memory]} (eng/components session)
        {:keys [id-to-node production-nodes query-nodes]} rulebase
        beta-nodes (for [[id node] id-to-node
                         :when (or (instance? JoinNode node)
                                   (instance? RootJoinNode node))]
                     node)

        accumulate-nodes (for [[id node] id-to-node
                         :when (instance? AccumulateNode node)]
                     node)

        ;; Get the counts for each beta node. Mutliple nodes may have the same facts
        ;; but merging these is benign since the counts would also match.
        fact-counts (reduce
                     (fn [fact-counts beta-node]
                       (let [facts (for [{:keys [fact]} (mem/get-elements-all memory beta-node)]
                                     fact)]
                         (merge fact-counts (frequencies facts))))
                     {}
                     beta-nodes)

        activations (->> (for [{:keys [node token]} (mem/get-activations memory)]
                           {(:id node) [token]})
                         (apply merge-with concat))

        accum-results (into {}
                            (for [accum-node accumulate-nodes]
                                 [(:id accum-node) (mem/get-accum-reduced-complete memory accum-node)]))

        insertions (into {}
                         (for [[id node] id-to-node
                               :when (instance? ProductionNode node)
                               token (mem/get-tokens-all memory node)]
                           [[(:id node) token] (mem/get-insertions memory node token)] ))]

    {:fact-counts fact-counts
     :activations (or activations {})
     :accum-results (or accum-results {})
     :insertions insertions}))

(defn- restore-activations
  "Restores the activations to the given session."
  [session {:keys [activations] :as session-state}]
  (let [{:keys [memory rulebase] :as components} (eng/components session)
        {:keys [production-nodes id-to-node]} rulebase

        restored-activations (for [[node-id tokens] activations
                                   token tokens]
                               (eng/->Activation (id-to-node node-id) token))

        grouped-by-production (group-by (fn [activation]
                                          (-> activation :node :production)) restored-activations)

        transient-memory (doto (mem/to-transient memory)
                           (mem/clear-activations!))]

    (doseq [[production activations] grouped-by-production]

      (mem/add-activations! transient-memory production activations))

    ;; Create a new session with the given activations.
    (eng/assemble (assoc components :memory (mem/to-persistent! transient-memory)))))

(defn- restore-accum-results
  [session {:keys [accum-results] :as session-state}]
  (let [{:keys [memory rulebase transport] :as components} (eng/components session)
        id-to-node (:id-to-node rulebase)
        transient-memory (mem/to-transient memory)]

    ;; Add the results to the accumulator node.
    (doseq [[id results] accum-results
            {:keys [join-bindings fact-bindings result]} results]

      (eng/right-activate-reduced (id-to-node id)
                                  join-bindings
                                  [[fact-bindings result]]
                                  transient-memory
                                  transport
                                  (l/to-transient l/default-listener)))

    (eng/assemble (assoc components :memory (mem/to-persistent! transient-memory)))))

(defn- restore-insertions
  [session {:keys [insertions] :as session-state}]
  (let [{:keys [memory rulebase transport] :as components} (eng/components session)
        id-to-node (:id-to-node rulebase)
        transient-memory (mem/to-transient memory)]

    ;; Add the results to the accumulator node.
    (doseq [[[id token] inserted-facts] insertions]

      (mem/add-insertions! transient-memory (id-to-node id) token inserted-facts))

    (eng/assemble (assoc components :memory (mem/to-persistent! transient-memory)))))

(sm/defn restore-session-state
  " Restore the given session to have the provided session state. The given session should be
   a newly-created session that was created with the same parameters as the session that was
   serialized. For instance, it should use the same rulesets, type function, and other settings.

   This function returns a new session instance that reflects the given saved state.  The returned
   session should be indistinguishable from the session that had its state saved."
  [session
   {:keys [fact-counts] :as session-state} :- session-state-schema]
  (let [fact-seq (for [[fact count] fact-counts
                       i (range count)]
                   fact)]

    (-> session
        (insert-all fact-seq)
        (restore-activations session-state)
        (restore-accum-results session-state)
        (restore-insertions session-state))))
