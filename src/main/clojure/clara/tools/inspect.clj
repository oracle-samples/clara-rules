(ns clara.tools.inspect
  "Tooling to inspect a rule session."
  (:require [clara.rules.compiler :as com]
            [clara.rules.engine :as eng]
            [clara.rules.schema :as schema]
            [clara.rules.memory :as mem]
            [schema.core :as s]
            [schema.macros :as sm]))

;; A structured explanation of why a rule or query matched.
;; This is derived from the Rete-style tokens, but this token
;; is designed to propagate all context needed to easily inspect
;; the state of rules.
(sm/defrecord Explanation [matches :- [[(s/one s/Any "fact") (s/one schema/Condition "condition")]] ; Fact, condition tuples
                           bindings :- {s/Keyword s/Any}]) ; Bound variables

;; Schema of an inspected rule session.
(def InspectionSchema
  {:rule-matches {schema/Rule [Explanation]}
   :query-matches {schema/Query [Explanation]}
   :condition-matches {schema/Condition [s/Any]}})

(defn- get-condition-matches
  "Returns facts matching each condition"
  [beta-roots memory]
  (let [join-nodes (for [beta-root beta-roots
                         beta-node (tree-seq :children :children beta-root)
                         :when (= :join (:node-type beta-node))]
                     beta-node)]
    (reduce
     (fn [matches node]
       (update-in matches
                  [(:condition node)]
                  concat (map :fact (mem/get-elements-all memory node))))
     {}
     join-nodes)))

(defn- to-explanations
  "Helper function to convert tokens to explanation records."
  [session tokens]
  (let [id-to-node (get-in (eng/components session) [:rulebase :id-to-node])]
       (for [{:keys [matches bindings]} tokens]
         (->Explanation
          ;; Convert matches to explanation structure.
          (for [[fact node-id] matches]
            [fact (-> node-id (id-to-node) (:condition))])
          bindings))))

(sm/defn inspect
  "Returns a representation of the given rule session useful to understand the
   state of the underlying rules.

   The returned structure includes the following keys:

   * :rule-matches -- a map of rule structures to their matching explanations.
   * :query-matches -- a map of query structures to their matching explanations.
   * :condition-matches -- a map of conditions pulled from each rule to facts they match.

   Users may inspect the entire structure for troubleshooting or explore it
   for specific cases. For instance, the following code snippet could look
   at all matches for some example rule:

   (defrule example-rule ... )

   ...

   (get-in (inspect example-session) [:rule-matches example-rule])

   ...

   The above segment will return matches for the rule in question."
  [session] :- InspectionSchema
  (let [{:keys [memory rulebase]} (eng/components session)
        {:keys [productions production-nodes query-nodes]} rulebase

        beta-tree (com/to-beta-tree productions)

        ;; Map of queries to their nodes in the network.
        query-to-nodes (into {} (for [[query-name query-node] query-nodes]
                                  [(:query query-node) query-node]))

        ;; Map of rules to their nodes in the network.
        rule-to-nodes (into {} (for [rule-node production-nodes]
                                 [(:production rule-node) rule-node]))]

    {:rule-matches (into {}
                          (for [[rule rule-node] rule-to-nodes]
                            [rule (to-explanations session
                                                   (mem/get-tokens-all memory rule-node))]))

     :query-matches (into {}
                          (for [[query query-node] query-to-nodes]
                            [query (to-explanations session
                                                    (mem/get-tokens-all memory query-node))]))

     :condition-matches (get-condition-matches beta-tree memory)
     }))

(defn explain-activation
  "Prints a human-readable explanation of the facts and conditions that created the Rete token."
  ([explanation] (explain-activation explanation ""))
  ([explanation prefix]
     (doseq [[fact condition] (:matches explanation)]
       (if (:from condition)
         ;; Explain why the accumulator matched.
         (let [{:keys [accumulator from]} condition]
           (println prefix fact)
           (println prefix "  accumulated with" accumulator)
           (println prefix "  from" (:type from))
           (println prefix "  where" (:constraints from)))

         ;; Explain why a condition matched.
         (let [{:keys [type constraints]} condition]
           (println prefix fact)
           (println prefix "  is a" type)
           (println prefix "  where" constraints))))))

(defn explain-activations
  "Prints a human-friend explanation of why rules and queries matched in the given session."
  [session]
  (doseq [[rule explanations] (:rule-matches (inspect session))
          :when (seq explanations)]
    (println "rule"  (or (:name rule) (str "<" (:lhs rule) ">")))
    (println "  executed")
    (println "   " (:rhs rule))
    (doseq [explanation explanations]
      (println "  because")
      (explain-activation explanation "    "))
    (println))

  (doseq [[rule explanations] (:query-matches (inspect session))
          :when (seq explanations)]
    (println "query"  (or (:name rule) (str "<" (:lhs rule) ">")))
    (doseq [explanation explanations]
      (println "  qualified because")
      (explain-activation explanation "    "))
    (println)))
