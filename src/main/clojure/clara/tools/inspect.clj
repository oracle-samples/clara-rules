(ns clara.tools.inspect
  "Tooling to inspect a rule session."
  (:require [clara.rules.compiler :as com]
            [clara.rules.engine :as eng]
            [clara.rules.schema :as schema]
            [clara.rules.memory :as mem]
            [schema.core :as s]
            [schema.macros :as sm]))

(defn- get-condition-matches 
  "Returns facts matching each condition"
  [beta-roots memory]
  ;; TODO: add inspection of query, test, and negation nodes?
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

(defn inspect 
  "Returns a representation of the given rule session useful to understand the
   state of the underlying rules.

   The returned structure includes the following keys:

   * :rule-matches -- a map of rule structures to their matching Rete tokens.
   * :query-matches -- a map of query structures to their matching Rete tokens.
   * :condition-matches -- a map of conditions pulled from each rule to facts they match.

   Users may inspect the entire structure for troubleshooting or explore it
   for specific cases. For instance, the following code snippet could look
   at all matches for some example rule:
   
   (defrule example-rule ... )

   ...

   (get-in (inspect example-session) [:rule-matches example-rule])

   ...

   The above segment will return matches for the rule in question."
  [session]
  (let [memory (eng/working-memory session)
        {:keys [productions production-nodes query-nodes]} (mem/get-rulebase memory)

        beta-tree (com/to-beta-tree productions)

        ;; Map of queries to their nodes in the network.
        query-to-nodes (into {} (for [[query-name query-node] query-nodes]
                                  [(:query query-node) query-node])) 

        ;; Map of rules to their nodes in the network.
        rule-to-nodes (into {} (for [rule-node production-nodes]
                                 [(:production rule-node) rule-node]))]

    {:rule-matches (into {}
                          (for [[rule rule-node] rule-to-nodes]
                            [rule (mem/get-tokens-all memory rule-node)]))

     :query-matches (into {}
                          (for [[query query-node] query-to-nodes]                
                            [query (mem/get-tokens-all memory query-node)]))

     :condition-matches (get-condition-matches beta-tree memory)
     }))

