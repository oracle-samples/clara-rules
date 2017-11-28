(ns clara.tools.fact-graph
  (:require [clara.tools.inspect :as i]
            [schema.core :as sc]))

;; This node will have either facts or results of accumulations as its parents.
;; Its children will be facts that the rule inserted.
(sc/defrecord RuleActivationNode [rule-name :- sc/Str
                                  id :- sc/Int])

;; The parents of this node are facts over which an accumulation was run.
;; It will have a single child, the result of the accumulation.  So, for example,
;; with the condition [?t <- (acc/min :temperature) :from [Temperature]], if we have
;; (->Temperature 50 "MCI") and (->Temperature 30 "MCI") the child of this node will be
;; an AccumulationResult with the :result 30 and the parents will be the two Temperature facts.
(sc/defrecord AccumulationNode [id :- sc/Int])

;; As alluded to above, this node represents the result of an accumulation.  Its child will be a
;; RuleActivationNode.  Note that there will be an AccumulationResult for each distinct rules firing.
(sc/defrecord AccumulationResultNode [id :- sc/Int
                                      result :- sc/Any])

(def ^:private empty-fact-graph {:forward-edges {}
                                 :backward-edges {}})

(defn ^:private add-edge [graph from to]
  (-> graph
      (update-in [:forward-edges from] (fnil conj #{}) to)
      (update-in [:backward-edges to] (fnil conj #{}) from)))

(defn ^:private add-insertion-to-graph
  [original-graph id-counter fact-inserted {:keys [rule-name explanation]}]
  (let [facts-direct (sequence
                      (comp (remove :facts-accumulated)
                            (map :fact))
                      (:matches explanation))
        
        activation-node (map->RuleActivationNode {:rule-name rule-name
                                                  :id (swap! id-counter inc)})

        accum-matches (filter :facts-accumulated (:matches explanation))]
    
    (as-> original-graph graph
      (if (seq accum-matches)
        (reduce (fn [reduce-graph accum-match]
                  (let [accum-node (->AccumulationNode (swap! id-counter inc))
                        accum-result (->AccumulationResultNode (swap! id-counter inc) (:fact accum-match))]
                    (as-> reduce-graph g
                      ;; Add edges from individual facts to an AccumulationResultNode.
                      (reduce (fn [g accum-element]
                                (add-edge g accum-element accum-node))
                              g (:facts-accumulated accum-match))
                      (add-edge g accum-node accum-result)
                      (add-edge g accum-result activation-node))))
                graph
                accum-matches)
        graph)
      ;; Add edges to the rule activation node from the facts that contributed
      ;; to the rule firing that were not in accumulator condition.
      (reduce (fn [g f]
                (add-edge g f activation-node))
              graph
              facts-direct)
      (add-edge graph activation-node fact-inserted))))

(defn session->fact-graph
  "Given a session, return a graph structure connecting all facts to the facts
   that they caused to be logically inserted.  Note that such connections will not
   be made for unconditionally inserted facts."
  [session]
  (let [id-counter (atom 0)
        ;; Use a counter, whose value will be added to internal nodes, to the ensure that
        ;; these nodes are not equal to each other.  This ensures that the number of the internal
        ;; nodes accurately reflects the cardinality of each fact in the session.

        ;; This function generates one of the entries in the map returned by clara.tools.inspect/inspect.
        ;; The function is private since it would be confusing for only one of the entries in clara.tools.inspect/inspect
        ;; to be accessible without generating the entire session inspection map.  However, we want to reuse the functionality
        ;; here without the performance penalty of generating all of the inspection data for the session.  Therefore, for now
        ;; we break the privacy of the function here.  Once issue 286 is completed we should remove this private Var access.
        fact->explanations (@#'i/gen-fact->explanations session)
        
        ;; Produce tuples of the form [inserted-fact {:rule rule :explanation clara.tools.inspect.Explanation}]
        insertion-tuples (into []
                               (comp
                                (map (fn [[fact v]]
                                       (map (fn [{:keys [rule explanation]}]
                                              [fact {:rule-name (:name rule)
                                                     :explanation explanation}])
                                            v)))
                                cat)
                               fact->explanations)]
    
    (reduce (fn [graph tuple]
              (apply add-insertion-to-graph graph id-counter tuple))
            empty-fact-graph
            insertion-tuples)))
