(ns clara.rules
  "Forward-chaining rules for Clojure. The primary API is in this namespace."
  (:require [clara.rules.engine :as eng]
            [schema.core :as s]
            [clara.rules.platform :as platform]
            #?(:cljs [clara.rules.listener :as l])
            #?(:clj [clara.rules.compiler :as com])
            #?(:clj [clara.rules.dsl :as dsl]))
  #?(:cljs (:require-macros clara.rules)))

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

   The query itself may be either the var created by a defquery statement,
   or the actual name of the query.
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

(defn insert-all-unconditional!
  "Behaves the same as insert-unconditional!, but accepts a sequence of facts to be inserted rather than individual facts.

   See the doc in insert-unconditional! for details on uncondotional insert behavior."
  [facts]
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
  (eng/rhs-retract-facts! facts))

(defn accumulate
  "DEPRECATED. Use clara.rules.accumulators/accum instead.

  Creates a new accumulator based on the given properties:

   * An initial-value to be used with the reduced operations.
   * A reduce-fn that can be used with the Clojure Reducers library to reduce items.
   * An optional combine-fn that can be used with the Clojure Reducers library to combine reduced items.
   * An optional retract-fn that can remove a retracted fact from a previously reduced computation
   * An optional convert-return-fn that converts the reduced data into something useful to the caller.
     Simply uses identity by default.
    "
  [& {:keys [initial-value reduce-fn combine-fn retract-fn convert-return-fn] :as args}]
  (eng/map->Accumulator
   (merge {;; Default conversion does nothing, so use identity.
           :convert-return-fn identity}
          args)))

#?(:cljs
  (defrecord Rulebase [alpha-roots beta-roots productions queries production-nodes query-nodes id-to-node]))

#?(:cljs
  (defn- create-get-alphas-fn
    "Returns a function that given a sequence of facts,
    returns a map associating alpha nodes with the facts they accept."
    [fact-type-fn ancestors-fn merged-rules]

    ;; We preserve a map of fact types to alpha nodes for efficiency,
    ;; effectively memoizing this operation.
    (let [alpha-map (atom {})]
      (fn [facts]
        (for [[fact-type facts] (platform/tuned-group-by fact-type-fn facts)]

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
              [new-nodes facts])))))))

#?(:cljs
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
          :id-to-node id-to-node}))))


#?(:cljs
  (defn assemble-session
  "This is used by tools to create a session; most users won't use this function."
  [beta-roots alpha-fns productions options]
  (let [rulebase (mk-rulebase beta-roots alpha-fns productions)
        transport (eng/LocalTransport.)

        ;; The fact-type uses Clojure's type function unless overridden.
        fact-type-fn (or (get options :fact-type-fn)
                         type)

        ;; The ancestors for a logical type uses Clojurescript's ancestors function unless overridden.
        ancestors-fn (or (get options :ancestors-fn)
                         ancestors)

        ;; Create a function that groups a sequence of facts by the collection
        ;; of alpha nodes they target.
        ;; We cache an alpha-map for facts of a given type to avoid computing
        ;; them for every fact entered.
        get-alphas-fn (create-get-alphas-fn fact-type-fn ancestors-fn rulebase)

        activation-group-sort-fn (eng/options->activation-group-sort-fn options)

        activation-group-fn (eng/options->activation-group-fn options)

        listener (if-let [listeners (:listeners options)]
                   (l/delegating-listener listeners)
                   l/default-listener)]

    ;; ClojureScript implementation doesn't support salience yet, so
    ;; no activation group functions are used.
    (eng/LocalSession. rulebase (eng/local-memory rulebase transport activation-group-sort-fn activation-group-fn get-alphas-fn) transport listener get-alphas-fn))))

#?(:clj
   (extend-type clojure.lang.Symbol
     com/IRuleSource
     (load-rules [sym]
       ;; Find the rules and queries in the namespace, shred them,
       ;; and compile them into a rule base.
       (if (namespace sym)
         ;; The symbol is qualified, so load rules in the qualified symbol.
         (let [resolved (resolve sym)]
           (when (nil? resolved)
             (throw (ex-info (str "Unable to resolve rule source: " sym) {:sym sym})))

           (cond
             ;; The symbol references a rule or query, so just return it
             (or (:query (meta resolved))
                 (:rule (meta resolved))) [@resolved]

             ;; The symbol refernces a sequence, so return it.
             (sequential? @resolved) @resolved

             :default
             (throw (ex-info (str "The source referenced by " sym " is not valid.") {:sym sym} ))))

         ;; The symbol is not qualified, so treat it as a namespace.
         (->> (ns-interns sym)
              (vals) ; Get the references in the namespace.
              (filter var?)
              (filter (comp (some-fn :rule :query :production-seq) meta)) ; Filter down to rules, queries, and seqs of both.
              ;; If definitions are created dynamically (i.e. are not reflected in an actual code file)
              ;; it is possible that they won't have :line metadata, so we have a default of 0.
              (sort (fn [v1 v2]
                      (compare (or (:line (meta v1)) 0)
                               (or (:line (meta v2)) 0))))
              (mapcat #(if (:production-seq (meta %))
                         (deref %)
                         [(deref %)])))))))

#?(:clj
  (defmacro mk-session
     "Creates a new session using the given rule sources. The resulting session
      is immutable, and can be used with insert, retract, fire-rules, and query functions.

      If no sources are provided, it will attempt to load rules from the caller's namespace,
      which is determined by reading Clojure's *ns* var.

      This will use rules defined with defrule, queries defined with defquery, and sequences
      of rule and/or query structures in vars that are annotated with the metadata ^:production-seq.

      The caller may also specify keyword-style options at the end of the parameters. Currently five
      options are supported, although most users will either not need these or just the first two:

      * :fact-type-fn, which must have a value of a function used to determine the logical type of a given
        cache. Defaults to Clojure's type function.
      * :cache, indicating whether the session creation can be cached, effectively memoizing mk-session.
        Defaults to true. Callers may wish to set this to false when needing to dynamically reload rules.
      * :ancestors-fn, which returns a sequence of ancestors for a given type. Defaults to Clojure's ancestors function. A
        fact of a given type will match any rule that uses one of that type's ancestors.
      * :activation-group-fn, a function applied to production structures and returns the group they should be activated with.
        It defaults to checking the :salience property, or 0 if none exists.
      * :activation-group-sort-fn, a comparator function used to sort the values returned by the above :activation-group-fn.
        Defaults to >, so rules with a higher salience are executed first.

      This is not supported in ClojureScript, since it requires eval to dynamically build a session. ClojureScript
      users must use pre-defined rule sessions using defsession."
     [& args]
     (if (and (seq args) (not (keyword? (first args))))
       `(com/mk-session ~(vec args)) ; At least one namespace given, so use it.
       `(com/mk-session (concat [(ns-name *ns*)] ~(vec args)))))) ; No namespace given, so use the current one.

#?(:clj
  (defmacro defsession
    "Creates a sesson given a list of sources and keyword-style options, which are typically Clojure namespaces.

    Typical usage would be like this, with a session defined as a var:

    (defsession my-session 'example.namespace)

    That var contains an immutable session that then can be used as a starting point to create sessions with
    caller-provided data. Since the session itself is immutable, it can be safely used from multiple threads
    and will not be modified by callers. So a user might grab it, insert facts, and otherwise
    use it as follows:

    (-> my-session
     (insert (->Temperature 23))
     (fire-rules))"
    [name & sources-and-options]
    (if (com/compiling-cljs?)
      `(clara.macros/defsession ~name ~@sources-and-options)
      `(def ~name (com/mk-session ~(vec sources-and-options))))))

#?(:clj
  (defmacro defrule
    "Defines a rule and stores it in the given var. For instance, a simple rule would look like this:

    (defrule hvac-approval
      \"HVAC repairs need the appropriate paperwork, so insert
        a validation error if approval is not present.\"
      [WorkOrder (= type :hvac)]
      [:not [ApprovalForm (= formname \"27B-6\")]]
      =>
      (insert! (->ValidationError
                :approval
                \"HVAC repairs must include a 27B-6 form.\")))

See the [rule authoring documentation](http://www.clara-rules.org/docs/rules/) for details."
    [name & body]
    (if (com/compiling-cljs?)
      `(clara.macros/defrule ~name ~@body)
      (let [doc (if (string? (first body)) (first body) nil)
            body (if doc (rest body) body)
            properties (if (map? (first body)) (first body) nil)
            definition (if properties (rest body) body)
            {:keys [lhs rhs]} (dsl/split-lhs-rhs definition)]
        (when-not rhs
          (throw (ex-info (str "Invalid rule " name ". No RHS (missing =>?).")
                          {})))
        `(def ~(vary-meta name assoc :rule true :doc doc)
           (cond-> ~(dsl/parse-rule* lhs rhs properties {} (meta &form))
             ~name (assoc :name ~(str (clojure.core/name (ns-name *ns*)) "/" (clojure.core/name name)))
             ~doc (assoc :doc ~doc)))))))

#?(:clj
  (defmacro defquery
    "Defines a query and stored it in the given var. For instance, a simple query that accepts no
parameters would look like this:

    (defquery check-job
      \"Checks the job for validation errors.\"
      []
      [?issue <- ValidationError])

See the [query authoring documentation](http://www.clara-rules.org/docs/queries/) for details."
    [name & body]
    (if (com/compiling-cljs?)
      `(clara.macros/defquery ~name ~@body)
      (let [doc (if (string? (first body)) (first body) nil)
            binding (if doc (second body) (first body))
            definition (if doc (drop 2 body) (rest body) )]
        `(def ~(vary-meta name assoc :query true :doc doc)
           (cond-> ~(dsl/parse-query* binding definition {} (meta &form))
             ~name (assoc :name ~(str (clojure.core/name (ns-name *ns*)) "/" (clojure.core/name name)))
             ~doc (assoc :doc ~doc)))))))
