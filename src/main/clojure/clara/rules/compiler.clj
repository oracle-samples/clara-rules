(ns clara.rules.compiler
  "The Clara rules compiler, translating raw data structures into compiled versions and functions.
   Most users should use only the clara.rules namespace."
  (:require
    [clara.rules.engine :as eng]
    [clara.rules.compiler.helpers :as hlp]
    [clara.rules.compiler.codegen :as codegen]
    [clara.rules.compiler.expressions :as expr]
    [clara.rules.compiler.trees :as trees]
    [clara.rules.platform :as platform]
    [clara.rules.schema :as schema]
    [clara.rules.engine :as eng]
    [clara.rules.engine.nodes :as nodes]
    [clara.rules.engine.nodes.accumulators :as accs]
    [clara.rules.engine.transports :as transports]
    [clojure.set :as s] [clojure.string :as string]
    [schema.core :as sc] [schema.macros :as sm])

  (:import [clara.rules.engine.nodes ProductionNode QueryNode JoinNode NegationNode TestNode
                                AlphaNode]
           [clara.rules.engine.nodes.accumulators AccumulateNode]
           [clara.rules.engine.transports LocalTransport]
           [clara.rules.engine.wme Accumulator]))

(defn- compile-constraints [exp-seq assigment-set]
  (if (empty? exp-seq)
    `((deref ~'?__bindings__))
    (let [ [[cmp a b :as exp] & rest] exp-seq
           compiled-rest (compile-constraints rest assigment-set)
           a-in-assigment (and expr/is-equals? (and (symbol? a) (assigment-set (keyword a))))
           b-in-assigment (and expr/is-equals? (and (symbol? b) (assigment-set (keyword b))))]
      (cond
       a-in-assigment
       (if b-in-assigment
         `((let [a-exist# (contains? (deref ~'?__bindings__) ~(keyword a))
                 b-exist# (contains? (deref ~'?__bindings__) ~(keyword b))]
             (when (and (not a-exist#) (not b-exist#)) (throw (Throwable. "Binding undefine variables")))
             (when (not a-exist#) (swap! ~'?__bindings__ assoc ~(keyword a) ((deref ~'?__bindings__) ~(keyword b))))
             (when (not b-exist#) (swap! ~'?__bindings__ assoc ~(keyword b) ((deref ~'?__bindings__) ~(keyword a))))
             (if (or (not a-exist#) (not b-exist#) (= ((deref ~'?__bindings__) ~(keyword a)) ((deref ~'?__bindings__) ~(keyword b))))
               (do ~@compiled-rest)
               nil)))
         (cons `(swap! ~'?__bindings__ assoc ~(keyword a) ~b) compiled-rest))
       b-in-assigment
       (cons `(swap! ~'?__bindings__ assoc ~(keyword b) ~a) compiled-rest)
       ;; not a unification
       :else
       (list (list 'if exp (cons 'do compiled-rest) nil))))))

(defn compile-condition
  "Returns a function definition that can be used in alpha nodes to test the condition."
  [type destructured-fact constraints result-binding env]
  (let [;; Get a map of fieldnames to access function symbols.
        accessors (hlp/get-fields type)

        binding-keys (expr/variables-as-keywords constraints)
        ;; The assignments should use the argument destructuring if provided, or default to accessors otherwise.
        assignments (if destructured-fact
                      ;; Simply destructure the fact if arguments are provided.
                      [destructured-fact '?__fact__]
                      ;; No argument provided, so use our default destructuring logic.
                      (concat '(this ?__fact__)
                              (mapcat (fn [[name accessor]]
                                        [name (list accessor '?__fact__)])
                                      accessors)))

        ;; The destructured environment, if any
        destructured-env (if (> (count env) 0)
                           {:keys (mapv #(symbol (name %)) (keys env))}
                           '?__env__)

        ;; Initial bindings used in the return of the compiled condition expresion.
        initial-bindings (if result-binding {result-binding '?__fact__}  {})]

    `(fn [~(clara.rules.compiler.helpers/add-meta '?__fact__ type)
          ~destructured-env] ;; TODO: add destructured environment parameter...
       (let [~@assignments
             ~'?__bindings__ (atom ~initial-bindings)]
         (do ~@(compile-constraints constraints (set binding-keys)))))))

;; FIXME: add env...
(defn compile-test [tests]
  (let [binding-keys (expr/variables-as-keywords tests)
        assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)]

    `(fn [~'?__token__]
      (let [~@assignments]
        (and ~@tests)))))

(defn compile-action
  "Compile the right-hand-side action of a rule, returning a function to execute it."
  [binding-keys rhs env]
  (let [assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)

        ;; The destructured environment, if any.
        destructured-env (if (> (count env) 0)
                           {:keys (mapv #(symbol (name %)) (keys env))}
                           '?__env__)]
    `(fn [~'?__token__  ~destructured-env]
       (let [~@assignments]
         ~rhs))))

(defn compile-accum
  "Used to create accumulators that take the environment into account."
  [accum env]
  (let [destructured-env
        (if (> (count env) 0)
          {:keys (mapv #(symbol (name %)) (keys env))}
          '?__env__)]
    `(fn [~destructured-env]
       ~accum)))

(defn compile-join-filter
  "Compiles to a predicate function that ensures the given items can be unified. Returns a ready-to-eval
   function that accepts a token, a fact, and an environment, and returns truthy if the given fact satisfies
   the criteria."
  [{:keys [type constraints args] :as unification-condition} env]
  (let [accessors (hlp/get-fields type)

        binding-keys (expr/variables-as-keywords constraints)

        destructured-env (if (> (count env) 0)
                           {:keys (mapv #(symbol (name %)) (keys env))}
                           '?__env__)

        destructured-fact (first args)

        fact-assignments (if destructured-fact
                           ;; Simply destructure the fact if arguments are provided.
                           [destructured-fact '?__fact__]
                           ;; No argument provided, so use our default destructuring logic.
                           (concat '(this ?__fact__)
                                   (mapcat (fn [[name accessor]]
                                             [name (list accessor '?__fact__)])
                                           accessors)))

        token-assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)

        assignments (concat
                     fact-assignments
                     token-assignments)]

    `(fn [~'?__token__ ~(clara.rules.compiler.helpers/add-meta '?__fact__ type) ~destructured-env]
      (let [~@assignments]
        (and ~@constraints)))))

(sc/defn compile-alpha-nodes :- [{:type sc/Any
                                  :alpha-fn sc/Any ;; TODO: is a function...
                                  (sc/optional-key :env) {sc/Keyword sc/Any}
                                  :children [sc/Num]}]
  [alpha-nodes :- [schema/AlphaNode]]
  (for [{:keys [condition beta-children env]} alpha-nodes
        :let [{:keys [type constraints fact-binding args]} condition
              cmeta (meta condition)]]

    (cond-> {:type (hlp/effective-type type)
             :alpha-fn (binding [*file* (or (:file cmeta) *file*)]
                         (eval (with-meta
                                 (compile-condition
                                  type (first args) constraints
                                  fact-binding env)
                                 (meta condition))))
             :children beta-children}
            env (assoc :env env))))

(sc/defn compile-beta-tree
  "Compile the beta tree to the nodes used at runtime."
  ([beta-nodes  :- [schema/BetaNode]
    parent-bindings]
     (compile-beta-tree beta-nodes parent-bindings false))
  ([beta-nodes  :- [schema/BetaNode]
    parent-bindings
    is-root]
     (vec
      (for [beta-node beta-nodes
            :let [{:keys [condition children id production query join-bindings]} beta-node

                  ;; If the condition is symbol, attempt to resolve the clas it belongs to.
                  condition (if (symbol? condition)
                              (.loadClass (clojure.lang.RT/makeClassLoader) (name condition))
                              condition)

                  constraint-bindings (expr/variables-as-keywords (:constraints condition))

                  ;; Get all bindings from the parent, condition, and returned fact.
                  all-bindings (cond-> (s/union parent-bindings constraint-bindings)
                                       ;; Optional fact binding from a condition.
                                       (:fact-binding condition) (conj (:fact-binding condition))
                                       ;; Optional accumulator result.
                                       (:result-binding beta-node) (conj (:result-binding beta-node)))]]

        (case (:node-type beta-node)

          :join
          ;; Use an specialized root node for efficiency in this case.
          (if is-root
            (nodes/->RootJoinNode
             id
             condition
             (compile-beta-tree children all-bindings)
             join-bindings)
            (nodes/->JoinNode
             id
             condition
             (compile-beta-tree children all-bindings)
             join-bindings))

          :negation
          ;; Check to see if the negation includes an
          ;; expression that must be joined to the incoming token
          ;; and use the appropriate node type.
          (if (:join-filter-expressions beta-node)

            (nodes/->NegationWithJoinFilterNode
             id
             condition
             (eval (compile-join-filter (:join-filter-expressions beta-node) (:env beta-node)))
             (compile-beta-tree children all-bindings)
             join-bindings)

            (nodes/->NegationNode
             id
             condition
             (compile-beta-tree children all-bindings)
             join-bindings))

          :test
          (nodes/->TestNode
           id
           (eval (compile-test (:constraints condition)))
           (compile-beta-tree children all-bindings))

          :accumulator
          ;; We create an accumulator that accepts the environment for the beta node
          ;; into its context, hence the function with the given environment.
          (let [compiled-accum ((eval (compile-accum (:accumulator beta-node) (:env beta-node))) (:env beta-node))]

            ;; Ensure the compiled accumulator has the expected structure
            (when (not (instance? Accumulator compiled-accum))
              (throw (IllegalArgumentException. (str (:accumulator beta-node) " is not a valid accumulator."))))

            ;; If a non-equality unification is in place, compile the predicate and use
            ;; the specialized accumulate node.

            (if (:join-filter-expressions beta-node)

              (accs/->AccumulateWithJoinFilterNode
               id
               ;; Create an accumulator structure for use when examining the node or the tokens
               ;; it produces.
               {:accumulator (:accumulator beta-node)
                ;; Include the original filter expressions in the constraints for inspection tooling.
                :from (update-in condition [:constraints]
                                 into (-> beta-node :join-filter-expressions :constraints))}
               compiled-accum
               (eval (compile-join-filter (:join-filter-expressions beta-node) (:env beta-node)))
               (:result-binding beta-node)
               (compile-beta-tree children all-bindings)
               join-bindings)

              ;; All unification is based on equality, so just use the simple accumulate node.
              (accs/->AccumulateNode
               id
               ;; Create an accumulator structure for use when examining the node or the tokens
               ;; it produces.
               {:accumulator (:accumulator beta-node)
                :from condition}
               compiled-accum
               (:result-binding beta-node)
               (compile-beta-tree children all-bindings)
               join-bindings)))

          :production
          (nodes/->ProductionNode
           id
           production
           (binding [*file* (:file (meta (:rhs production)))]
             (eval (with-meta
                     (compile-action
                      all-bindings (:rhs production) (:env production))
                     (meta (:rhs production))))))

          :query
          (nodes/->QueryNode
           id
           query
           (:params query))
          )))))



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
            [new-nodes facts]))))))


;; Cache of sessions for fast reloading.
(def ^:private session-cache (atom {}))

(defn clear-session-cache!
  "Clears the cache of reusable Clara sessions, so any subsequent sessions
   will be re-compiled from the rule definitions. This is intended for use
   by tooling or specialized needs; most users can simply specify the :cache false
   option when creating sessions."
  []
  (reset! session-cache {}))

(sc/defn mk-session*
  "Compile the rules into a rete network and return the given session."
  [productions :- [schema/Production]
   options :- {sc/Keyword sc/Any}]
  (let [beta-struct (trees/to-beta-tree productions)
        beta-tree (compile-beta-tree beta-struct #{} true)
        alpha-nodes (compile-alpha-nodes (trees/to-alpha-tree beta-struct))
        rulebase (codegen/build-network beta-tree alpha-nodes productions)
        transport (LocalTransport.)

        ;; The fact-type uses Clojure's type function unless overridden.
        fact-type-fn (get options :fact-type-fn type)

        ;; The ancestors for a logical type uses Clojure's ancestors function unless overridden.
        ancestors-fn (get options :ancestors-fn ancestors)

        ;; Default sort by higher to lower salience.
        activation-group-sort-fn (get options :activation-group-sort-fn >)

        ;; Activation groups use salience, with zero
        ;; as the default value.
        activation-group-fn (get options
                                 :activation-group-fn
                                 (fn [production]
                                   (or (some-> production :props :salience)
                                       0)))

        ;; Create a function that groups a sequence of facts by the collection
        ;; of alpha nodes they target.
        ;; We cache an alpha-map for facts of a given type to avoid computing
        ;; them for every fact entered.
        get-alphas-fn (create-get-alphas-fn fact-type-fn ancestors-fn rulebase)]

    (eng/assemble {:rulebase rulebase
                  :memory (eng/local-memory rulebase transport activation-group-sort-fn activation-group-fn)
                  :transport transport
                  :listeners (get options :listeners  [])
                  :get-alphas-fn get-alphas-fn})))

(defn mk-session
  "Creates a new session using the given rule source. Thew resulting session
   is immutable, and can be used with insert, retract, fire-rules, and query functions."
  ([sources-and-options]

     ;; If an equivalent session has been created, simply reuse it.
     ;; This essentially memoizes this function unless the caller disables caching.
     (if-let [session (get @session-cache [sources-and-options])]
       session

       ;; Separate sources and options, then load them.
       (let [sources (take-while (complement keyword?) sources-and-options)
             options (apply hash-map (drop-while (complement keyword?) sources-and-options))
             productions (mapcat
                          #(if (satisfies? codegen/IRuleSource %)
                             (codegen/load-rules %)
                             %)
                          sources) ; Load rules from the source, or just use the input as a seq.
             session (mk-session* productions options)]

         ;; Cache the session unless instructed not to.
         (when (get options :cache true)
           (swap! session-cache assoc [sources-and-options] session))

         ;; Return the session.
         session))))
