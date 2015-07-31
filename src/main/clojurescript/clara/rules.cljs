(ns clara.rules
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem]
            [clara.rules.listener :as l]))

(defrecord Rulebase [alpha-roots beta-roots productions queries production-nodes query-nodes id-to-node])

(defn- create-get-alphas-fn
  "Returns a function that given a sequence of facts,
  returns a map associating alpha nodes with the facts they accept."
  [fact-type-fn ancestors-fn merged-rules]

  ;; We preserve a map of fact types to alpha nodes for efficiency,
  ;; effectively memoizing this operation.
  (let [alpha-map (atom {})]
    (fn [facts]
      (for [[fact-type facts] (group-by fact-type-fn facts)]

        (if-let [alpha-nodes (get @alpha-map fact-type)]

          ;; If the matching alpha nodes are cached, simply return them.
          [alpha-nodes facts]

          ;; The alpha nodes weren't cached for the type, so get them now.
          (let [ancestors (conj (ancestors-fn fact-type) fact-type)

                ;; Get all alpha nodes for all ancestors.
                new-nodes (distinct
                           (reduce
                            (fn [coll ancestor]
                              (concat
                               coll
                               (get-in merged-rules [:alpha-roots ancestor])))
                            []
                            ancestors))]

            (swap! alpha-map assoc fact-type new-nodes)
            [new-nodes facts]))))))

(defn- mk-rulebase
  [beta-roots alpha-fns productions]

    (let [beta-nodes (for [root beta-roots
                           node (tree-seq :children :children root)]
                       node)

          production-nodes (for [node beta-nodes
                                 :when (= eng/ProductionNode (type node))]
                             node)

          query-nodes (for [node beta-nodes
                            :when (= eng/QueryNode (type node))]
                        node)

          query-map (into {} (for [query-node query-nodes

                                   ;; Queries can be looked up by reference or by name;
                                   entry [[(:query query-node) query-node]
                                          [(:name (:query query-node)) query-node]]]
                               entry))

          ;; Map of node ids to beta nodes.
          id-to-node (into {} (for [node beta-nodes]
                                [(:id node) node]))

          ;; type, alpha node tuples.
          alpha-nodes (for [{:keys [type alpha-fn children env]} alpha-fns
                            :let [beta-children (map id-to-node children)]]
                        [type (eng/->AlphaNode env beta-children alpha-fn)])

          ;; Merge the alpha nodes into a multi-map
          alpha-map (reduce
                     (fn [alpha-map [type alpha-node]]
                       (update-in alpha-map [type] conj alpha-node))
                     {}
                     alpha-nodes)]

      (map->Rulebase
       {:alpha-roots alpha-map
        :beta-roots beta-roots
        :productions (filter :rhs productions)
        :queries (remove :rhs productions)
        :production-nodes production-nodes
        :query-nodes query-map
        :id-to-node id-to-node})))

(defn assemble-session
  "This is used by tools to create a session; most users won't use this function."
  [beta-roots alpha-fns productions options]
  (let [rulebase (mk-rulebase beta-roots alpha-fns productions)
        transport (eng/LocalTransport.)

        ;; The fact-type uses Clojure's type function unless overridden.
        fact-type-fn (get options :fact-type-fn type)

        ;; The ancestors for a logical type uses Clojurescript's ancestors function unless overridden.
        ancestors-fn (get options :ancestors-fn ancestors)

        ;; Create a function that groups a sequence of facts by the collection
        ;; of alpha nodes they target.
        ;; We cache an alpha-map for facts of a given type to avoid computing
        ;; them for every fact entered.
        get-alphas-fn (create-get-alphas-fn fact-type-fn ancestors-fn rulebase)

        ;; Default sort by higher to lower salience.
        activation-group-sort-fn (get options :activation-group-sort-fn >)

        ;; Activation groups use salience, with zero
        ;; as the default value.
        activation-group-fn (get options
                                 :activation-group-fn
                                 (fn [production]
                                   (or (some-> production :props :salience)
                                       0)))

        listener (if-let [listeners (:listeners options)]
                   (l/delegating-listener listeners)
                   l/default-listener)]

    ;; ClojureScript implementation doesn't support salience yet, so
    ;; no activation group functions are used.
    (eng/LocalSession. rulebase (eng/local-memory rulebase transport activation-group-sort-fn activation-group-fn) transport listener get-alphas-fn)))


(defn accumulate
  "Creates a new accumulator based on the given properties:

   * An initial-value to be used with the reduced operations.
   * A reduce-fn that can be used with the Clojure Reducers library to reduce items.
   * A combine-fn that can be used with the Clojure Reducers library to combine reduced items.
   * An optional retract-fn that can remove a retracted fact from a previously reduced computation
   * An optional convert-return-fn that converts the reduced data into something useful to the caller.
     Simply uses identity by default.
    "
  [& {:keys [initial-value reduce-fn combine-fn retract-fn convert-return-fn] :as args}]
  (eng/map->Accumulator
   (merge
    {:combine-fn reduce-fn ; Default combine function is simply the reduce.
     :convert-return-fn identity ; Default conversion does nothing, so use identity.
     :retract-fn (fn [reduced retracted] reduced) ; Retractions do nothing by default.
     }
    args)))

(defn insert
  "Inserts one or more facts into a working session. It does not modify the given
   session, but returns a new session with the facts added."
  [session & facts]
  (eng/insert session facts))

(defn insert-all
  "Inserts a sequence of facts into a working session. It does not modify the given
   session, but returns a new session with the facts added."
  [session fact-seq]
  (eng/insert session fact-seq))

(defn retract
  "Retracts a fact from a working session. It does not modify the given session,
   but returns a new session with the facts retracted."
  [session & facts]
  (eng/retract session facts))

(defn fire-rules
  "Fires are rules in the given session. Once a rule is fired, it is labeled in a fired
   state and will not be re-fired unless facts affecting the rule are added or retracted.

   This function does not modify the given session to mark rules as fired. Instead, it returns
   a new session in which the rules are marked as fired."
  [session]
  (eng/fire-rules session))

(defn query
  "Runs the given query with the optional given parameters against the session.
   The optional parameters should be in map form. For example, a query call might be:

   (query session get-by-last-name :last-name \"Jones\")
   "
  [session query & params]
  (eng/query session query (apply hash-map params)))

(defn insert!
  "To be executed within a rule's right-hand side, this inserts a new fact or facts into working memory.

   Inserted facts are logical, in that if the support for the insertion is removed, the fact
   will automatically be retracted. For instance, if there is a rule that inserts a \"Cold\" fact
   if a \"Temperature\" fact is below a threshold, and the \"Temperature\" fact that triggered
   the rule is retracted, the \"Cold\" fact the rule inserted is also retracted. This is the underlying
   truth maintenance facillity.

   This truth maintenance is also transitive: if a rule depends on some criteria to fire, and a
   criterion becomes invalid, it may retract facts that invalidate other rules, which in turn
   retract their conclusions. This way we can ensure that information inferred by rules is always
   in a consistent state."
  [& facts]
  (eng/insert-facts! facts false))

(defn insert-all!
  "Behaves the same as insert!, but accepts a sequence of facts to be inserted. This can be simpler and more efficient for
   rules needing to insert multiple facts.

   See the doc in insert! for details on insert behavior.."
  [facts]
  (eng/insert-facts! facts false))

(defn insert-unconditional!
  "To be executed within a rule's right-hand side, this inserts a new fact or facts into working memory.

   This differs from insert! in that it is unconditional. The facts inserted will not be retracted
   even if the rule activation doing the insert becomes false.  Most users should prefer the simple insert!
   function as described above, but this function is available for use cases that don't wish to use
   Clara's truth maintenance."
  [& facts]
  (eng/insert-facts! facts true))

(defn retract!
  "To be executed within a rule's right-hand side, this retracts a fact or facts from the working memory.

   Retracting facts from the right-hand side has slightly different semantics than insertion. As described
   in the insert! documentation, inserts are logical and will automatically be retracted if the rule
   that inserted them becomes false. This retract! function does not follow the inverse; retracted items
   are simply removed, and not re-added if the rule that retracted them becomes false.

   The reason for this is that retractions remove information from the knowledge base, and doing truth
   maintenance over retractions would require holding onto all retracted items, which would be an issue
   in some use cases. This retract! method is included to help with certain use cases, but unless you
   have a specific need, it is better to simply do inserts on the rule's right-hand side, and let
   Clara's underlying truth maintenance retract inserted items if their support becomes false."
  [& facts]
  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} eng/*current-session*]

    ;; Update the count so the rule engine will know when we have normalized.
    (swap! insertions + (count facts))

    (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
            root alpha-roots]

      (eng/alpha-retract root fact-group transient-memory transport listener))))
