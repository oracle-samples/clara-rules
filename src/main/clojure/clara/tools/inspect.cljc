(ns clara.tools.inspect
  "Tooling to inspect a rule session. The two major methods here are:

   * inspect, which returns a data structure describing the session that can be used by tooling.
   * explain-activations, which uses inspect and prints a human-readable description covering
     why each rule activation or query match occurred."
  (:require 
    #?(:clj [clara.rules.compiler.trees :as trees])
    [clara.rules.engine :as eng]
    [clara.rules.schema :as schema]
    [clara.rules.memory :as mem]
    #?(:clj [schema.core :as sc] :cljs [schema.core :as sc :include-macros true])))

;; A structured explanation of why a rule or query matched.
;; This is derived from the Rete-style tokens, but this token
;; is designed to propagate all context needed to easily inspect
;; the state of rules.
(sc/defrecord Explanation [matches :- [[(sc/one sc/Any "fact") (sc/one schema/Condition "condition")]] ; Fact, condition tuples
                            bindings :- {sc/Keyword sc/Any}]) ; Bound variables

;; Schema of an inspected rule session.
(def InspectionSchema
  {:rule-matches {schema/Rule [Explanation]}
   :query-matches {schema/Query [Explanation]}
   :condition-matches {schema/Condition [sc/Any]}
   :insertions {schema/Rule [{:explanation Explanation :fact sc/Any}]}})

(defn- get-condition-matches
  "Returns facts matching each condition"
  [beta-roots memory]
  (if (empty? beta-roots) {}
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
       join-nodes))))

(defn- to-explanations
  "Helper function to convert tokens to explanation records."
  [session tokens]
  (let [id-to-node (get-in (eng/components session) [:rulebase :id-to-node])]

    (for [{:keys [matches bindings]} tokens]
      (->Explanation
       ;; Convert matches to explanation structure.
       (for [[fact node-id] matches
             :let [node (id-to-node node-id)
                   condition (if (:accum-condition node)

                               {:accumulator (get-in node [:accum-condition :accumulator])
                                :from {:type (get-in node [:accum-condition :from :type])
                                       :constraints (or (seq (get-in node [:accum-condition :from :original-constraints]))
                                                        (get-in node [:accum-condition :from :constraints]))}}

                               {:type (:type (:condition node))
                                :constraints (or (seq (:original-constraints (:condition node)))
                                                 (:constraints (:condition node)))})]]
         [fact condition])

       ;; Remove generated bindings from user-facing explanation.
       (into {} (remove (fn [[k v]] (.startsWith (name k) "?__gen__")) bindings))))))

(sc/defn inspect
  " Returns a representation of the given rule session useful to understand the
   state of the underlying rules.

   The returned structure includes the following keys:

   * :rule-matches -- a map of rule structures to their matching explanations.
   * :query-matches -- a map of query structures to their matching explanations.
   * :condition-matches -- a map of conditions pulled from each rule to facts they match.
   * :insertions -- a map of rules to a sequence of {:explanation E, :fact F} records
     to allow inspection of why a given fact was inserted.

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
        beta-tree #?(:clj (trees/to-beta-tree productions) :cljs [])

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

     :insertions (into {}
                       (for [[rule rule-node] rule-to-nodes]
                         [rule
                          (for [token (mem/get-tokens-all memory rule-node)
                                insertion (mem/get-insertions memory rule-node token)]
                            {:explanation (first (to-explanations session [token])) :fact insertion})]))}))

(defn- explain-activation
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
  "Prints a human-friend explanation of why rules and queries matched in the given session.
  A caller my optionally pass a :rule-filter-fn, which is a predicate

  (clara.tools.inspect/explain-activations session
         :rule-filter-fn (fn [rule] (re-find my-rule-regex (:name rule))))"

  [session & {:keys [rule-filter-fn] :as options}]
  (let [filter-fn (or rule-filter-fn (constantly true))]

    (doseq [[rule explanations] (:rule-matches (inspect session))
            :when (filter-fn rule)
            :when (seq explanations)]
      (println "rule"  (or (:name rule) (str "<" (:lhs rule) ">")))
      (println "  executed")
      (println "   " (:rhs rule))
      (doseq [explanation explanations]
        (println "  with bindings")
        (println "    " (:bindings explanation))
        (println "  because")
        (explain-activation explanation "    "))
      (println))

    (doseq [[rule explanations] (:query-matches (inspect session))
            :when (filter-fn rule)
            :when (seq explanations)]
      (println "query"  (or (:name rule) (str "<" (:lhs rule) ">")))
      (doseq [explanation explanations]
        (println "  with bindings")
        (println "    " (:bindings explanation))
        (println "  qualified because")
        (explain-activation explanation "    "))
      (println))))
