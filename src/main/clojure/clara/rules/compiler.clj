(ns clara.rules.compiler
  "This namespace is for internal use and may move in the future.
   This is the Clara rules compiler, translating raw data structures into compiled versions and functions.
   Most users should use only the clara.rules namespace."
  (:require [clara.rules.engine :as eng]
            [clara.rules.schema :as schema]
            [clara.rules.platform :refer [jeq-wrap] :as platform]
            [clara.rules.hierarchy :as hierarchy]
            [clojure.core.cache.wrapped :as cache]
            [clj-commons.digest :as digest]
            [ham-fisted.api :as hf]
            [ham-fisted.set :as hs]
            [ham-fisted.mut-map :as hm]
            [futurama.util :as u]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [schema.core :as sc])
  (:import [clara.rules.platform
            JavaEqualityWrapper]
           [clara.rules.engine
            ProductionNode
            QueryNode
            AlphaNode
            RootJoinNode
            HashJoinNode
            ExpressionJoinNode
            NegationNode
            NegationWithJoinFilterNode
            TestNode
            AccumulateNode
            AccumulateWithJoinFilterNode
            LocalTransport
            Accumulator
            NegationResult
            ISystemFact]
           [java.beans
            PropertyDescriptor]
           [clojure.lang
            IFn]))

;; Cache of sessions for fast reloading.
(defonce default-session-cache
  (cache/lru-cache-factory {}))

;; Cache of compiled expressions
(defonce default-compiler-cache
  (cache/soft-cache-factory {}))

(defn clear-session-cache!
  "Clears the cache of reusable Clara sessions, so any subsequent sessions
   will be re-compiled from the rule definitions. This is intended for use
   by tooling or specialized needs; most users can simply specify the :cache false
   option when creating sessions."
  []
  (swap! default-session-cache empty))

(defn clear-compiler-cache!
  "Clears the default compiler cache of re-usable compiled expressions, so any
  subsequent expressions will be re-compiled. This is intended for use by tooling
  or during testing; most users can simply specify the :compiler-cache false
  option when creating sessions."
  []
  (swap! default-compiler-cache empty))

;; Protocol for loading rules from some arbitrary source.
(defprotocol IRuleSource
  (load-rules [source]))

(defprotocol IFactSource
  (load-facts [source]))

(defprotocol IHierarchySource
  (load-hierarchies [source]))

(sc/defschema BetaNode
  "These nodes exist in the beta network."
  (sc/pred (comp #{ProductionNode
                   QueryNode
                   RootJoinNode
                   HashJoinNode
                   ExpressionJoinNode
                   NegationNode
                   NegationWithJoinFilterNode
                   TestNode
                   AccumulateNode
                   AccumulateWithJoinFilterNode}
                 class)
           "Some beta node type"))

;; A rulebase -- essentially an immutable Rete network with a collection of
;; alpha and beta nodes and supporting structure.
(sc/defrecord Rulebase [;; Map of matched type to the alpha nodes that handle them.
                        alpha-roots :- {sc/Any [AlphaNode]}
                        ;; Root beta nodes (join, accumulate, etc.).
                        beta-roots :- [BetaNode]
                        ;; Productions in the rulebase.
                        productions :- #{schema/Production}
                        ;; Production nodes.
                        production-nodes :- [ProductionNode]
                        ;; Map of queries to the nodes hosting them.
                        query-nodes :- {sc/Any QueryNode}
                        ;; Map of id to one of the  alpha or beta nodes (join, accumulate, etc).
                        id-to-node :- {sc/Num (sc/conditional
                                               :activation AlphaNode
                                               :else BetaNode)}
                        ;; Function for sorting activation groups of rules for firing.
                        activation-group-sort-fn
                        ;; Function that takes a rule and returns its activation group.
                        activation-group-fn
                        ;; Function that takes facts and determines what alpha nodes they match.
                        get-alphas-fn
                        ;; A map of [node-id field-name] to function.
                        node-expr-fn-lookup :- schema/NodeFnLookup])

(defn- md5-hash
  "Returns the md5 digest of the given data after converting it to a string"
  [x]
  (digest/md5 ^String (pr-str x)))

(defn- is-variable?
  "Returns true if the given expression is a variable (a symbol prefixed by ?)"
  [expr]
  (and (symbol? expr)
       (.startsWith (name expr) "?")))

(defn- get-field-accessors
  "Given a clojure.lang.IRecord subclass, returns a map of field name to a
   symbol representing the function used to access it."
  [cls]
  (into {}
        (for [field-name (clojure.lang.Reflector/invokeStaticMethod ^Class cls
                                                                    "getBasis"
                                                                    ^"[Ljava.lang.Object;" (make-array Object 0))]
          ;; Do not preserve the metadata on the field names returned from
          ;; IRecord.getBasis() since it may not be safe to eval this metadata
          ;; in other contexts.  This mostly applies to :tag metadata that may
          ;; be unqualified class names symbols at this point.
          [(with-meta field-name {}) (symbol (str ".-" field-name))])))

(defn- get-bean-accessors
  "Returns a map of bean property name to a symbol representing the function used to access it."
  [cls]
  (into {}
        ;; Iterate through the bean properties, returning tuples and the corresponding methods.
        (for [^PropertyDescriptor property (seq (.. java.beans.Introspector
                                                    (getBeanInfo cls)
                                                    (getPropertyDescriptors)))
              :let [read-method (.getReadMethod property)]
              ;; In the event that there the class has an indexed property without a basic accessor we will simply skip
              ;; the accessor as we will not know how to retrieve the value. see https://github.com/cerner/clara-rules/issues/446
              :when read-method]
          [(symbol (string/replace (.getName property) #"_" "-")) ; Replace underscore with idiomatic dash.
           (symbol (str "." (.getName read-method)))])))

(defn- effective-type*
  [type]
  (if (symbol? type)
    (clojure.lang.RT/classForName ^String (name type))
    type))

(def effective-type
  (memoize effective-type*))

(defn- get-fields*
  "Returns a map of field name to a symbol representing the function used to access it."
  [type]
  (let [type (effective-type type)]
    (cond
      (isa? type clojure.lang.IRecord) (get-field-accessors type)
      (class? type) (get-bean-accessors type) ; Treat unrecognized classes as beans.
      :else [])))

(def get-fields
  (memoize get-fields*))

(defn- equality-expression? [expression]
  (let [qualify-when-sym #(when-let [resolved (and (symbol? %)
                                                   (resolve %))]
                            (and (var? resolved)
                                 (symbol (-> resolved meta :ns ns-name name)
                                         (-> resolved meta :name name))))
        op (first expression)]
    ;; Check for unqualified = or == to support original Clara unification
    ;; syntax where clojure.core/== was supposed to be excluded explicitly.
    (boolean (or (#{'= '== 'clojure.core/= 'clojure.core/==} op)
                 (#{'clojure.core/= 'clojure.core/==} (qualify-when-sym op))))))

(def ^:dynamic *compile-ctx* nil)

(def ^:dynamic *hierarchy* nil)

(defn try-eval
  "Evals the given `expr`.  If an exception is thrown, it is caught and an
   ex-info exception is thrown with more details added.  Uses *compile-ctx*
   for additional contextual info to add to the exception details."
  [expr]
  (try
    (eval expr)
    (catch Exception e
      (let [edata (merge {:expr expr}
                         (dissoc *compile-ctx* :msg))
            msg (:msg *compile-ctx*)]
        (throw (ex-info (str (if msg (str "Failed " msg) "Failed compiling.") \newline
                             ;; Put ex-data specifically in the string since
                             ;; often only ExceptionInfo.toString() will be
                             ;; called, which doesn't show this data.
                             edata \newline)
                        edata
                        e))))))

(defn- compile-constraints
  "Compiles a sequence of constraints into a structure that can be evaluated.

   Callers may also pass a collection of equality-only-variables, which instructs
   this function to only do an equality check on them rather than create a unification binding."
  ([exp-seq]
   (compile-constraints exp-seq #{}))
  ([exp-seq equality-only-variables]

   (if (empty? exp-seq)
     `(deref ~'?__bindings__)
     (let [[exp & rest-exp] exp-seq
           variables (into #{}
                           (filter (fn [item]
                                     (and (symbol? item)
                                          (= \? (first (name item)))
                                          (not (equality-only-variables item))))
                                   exp))
           expression-values (remove variables (rest exp))
           binds-variables? (and (equality-expression? exp)
                                 (seq variables))

           ;; if we intend on binding any variables at this level of the
           ;; expression then future layers should not be able to rebind them.
           ;; see https://github.com/cerner/clara-rules/issues/417 for more info
           equality-only-variables (if binds-variables?
                                     (into equality-only-variables
                                           variables)
                                     equality-only-variables)

           compiled-rest (compile-constraints rest-exp equality-only-variables)]

       (when (and binds-variables?
                  (empty? expression-values))
         (throw (ex-info (str "Malformed variable binding for " variables ". No associated value.")
                         {:variables (map keyword variables)})))

       (cond
         binds-variables?
         ;; Bind each variable with the first value we encounter.
         ;; The additional equality checks are handled below so which value
         ;; we bind to is not important. So an expression like (= ?x value-1 value-2) will
         ;; bind ?x to value-1, and then ensure value-1 and value-2 are equal below.

         ;; First assign each value in a let, so it is visible to subsequent expressions.
         `(let [~@(for [variable variables
                        let-expression [variable (first expression-values)]]
                    let-expression)]

            ;; Update the bindings produced by this expression.
            ~@(for [variable variables]
                `(swap! ~'?__bindings__ assoc ~(keyword variable) ~variable))

            ;; If there is more than one expression value, we need to ensure they are
            ;; equal as well as doing the bind. This ensures that value-1 and value-2 are
            ;; equal.
            ~(if (> (count expression-values) 1)

               `(if ~(cons '= expression-values) ~compiled-rest nil)
               ;; No additional values to check, so move on to the rest of
               ;; the expression
               compiled-rest))

;; A contraint that is empty doesn't need to be added as a check,
         ;; simply move on to the rest
         (empty? exp)
         compiled-rest

         ;; No variables to unify, so simply check the expression and
         ;; move on to the rest.
         :else
         `(if ~exp ~compiled-rest nil))))))

(defn flatten-expression
  "Flattens expression as clojure.core/flatten does, except will flatten
   anything that is a collection rather than specifically sequential."
  [expression]
  (filter (complement coll?)
          (tree-seq coll? seq expression)))

(defn variables-as-keywords
  "Returns a set of the symbols in the given s-expression that start with '?' as keywords"
  [expression]
  (into #{} (for [item (flatten-expression expression)
                  :when (is-variable? item)]
              (keyword  item))))

(defn field-name->accessors-used
  "Returns a map of field name to accessors for any field names of type used
   in the constraints."
  [type constraints]
  (let [field-name->accessor (get-fields type)
        all-fields (set (keys field-name->accessor))
        fields-used (into #{}
                          (filter all-fields)
                          (flatten-expression constraints))]
    (into {}
          (filter (comp fields-used key))
          field-name->accessor)))

(defn- add-meta
  "Helper function to add metadata."
  [fact-symbol fact-type]
  (let [fact-type (if (symbol? fact-type)
                    (try
                      (resolve fact-type)
                      (catch Exception e
                        ;; We shouldn't have to worry about exceptions being thrown here according
                        ;; to `resolve`s docs.
                        ;; However, due to http://dev.clojure.org/jira/browse/CLJ-1403 being open
                        ;; still, it is safer to catch any exceptions thrown.
                        fact-type))
                    fact-type)]
    (if (class? fact-type)
      (vary-meta fact-symbol assoc :tag (symbol (.getName ^Class fact-type)))
      fact-symbol)))

(defn mk-node-fn-name
  "A simple helper function to maintain a consistent pattern for naming anonymous functions in the rulebase.

   node-type - expected to align with one of the types of nodes defined in clara.rules.engine, and node-type->abbreviated-type.
   node-id - expected to be an integer
   fn-type - an identifier for what the function means to the node

   fn-type is required as some nodes might have multiple functions associated to them, ex. Accumulator nodes containing
   filter functions."
  [node-type node-id fn-type]
  (if-let [abbreviated-node-type (get eng/node-type->abbreviated-type node-type)]
    (with-meta (symbol (str abbreviated-node-type "-" fn-type)) {:node-id node-id})
    (throw (ex-info "Unrecognized node type"
                    {:node-type node-type
                     :node-id node-id
                     :fn-type fn-type}))))

(defn compile-condition
  "Returns a function definition that can be used in alpha nodes to test the condition."
  [type node-id destructured-fact constraints result-binding env]
  (let [;; Get a map of fieldnames to access function symbols.
        accessors (field-name->accessors-used type constraints)
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
        initial-bindings (if result-binding {result-binding '?__fact__}  {})

        ;; Hardcoding the node-type and fn-type as we would only ever expect 'compile-condition' to be used for this scenario
        fn-name (mk-node-fn-name "AlphaNode" node-id "AE")]
    `(fn ~fn-name [~(add-meta '?__fact__ type)
                   ~destructured-env]
       (let [~@assignments
             ~'?__bindings__ (atom ~initial-bindings)]
         ~(compile-constraints constraints)))))

(defn build-token-assignment
  "A helper function to build variable assignment forms for tokens."
  [binding-key]
  (list (symbol (name binding-key))
        (list `-> '?__token__ :bindings binding-key)))

(defn compile-test-handler [node-id constraints env]
  (let [binding-keys (variables-as-keywords constraints)
        assignments (mapcat build-token-assignment binding-keys)

        ;; The destructured environment, if any
        destructured-env (if (> (count env) 0)
                           {:keys (mapv #(symbol (name %)) (keys env))}
                           '?__env__)

        ;; Hardcoding the node-type and fn-type as we would only ever expect 'compile-test' to be used for this scenario
        fn-name (mk-node-fn-name "TestNode" node-id "TE")]
    `(fn ~fn-name [~'?__token__ ~destructured-env]
       (let [~@assignments]
         (and ~@constraints)))))

(defn compile-test [node-id constraints env]
  (let [test-handler (compile-test-handler node-id constraints env)]
    `(array-map :handler ~test-handler
                :constraints '~constraints)))

(defn compile-action-handler
  [action-name bindings-keys rhs env]
  (let [;; Avoid creating let bindings in the compile code that aren't actually used in the body.
        ;; The bindings only exist in the scope of the RHS body, not in any code called by it,
        ;; so this scanning strategy will detect all possible uses of binding variables in the RHS.
        ;; Note that some strategies with macros could introduce bindings, but these aren't something
        ;; we're trying to support.  If necessary a user could macroexpand their RHS code manually before
        ;; providing it to Clara.
        rhs-bindings-used (variables-as-keywords rhs)

        assignments (sequence
                     (comp
                      (filter rhs-bindings-used)
                      (mapcat build-token-assignment))
                     bindings-keys)

        ;; The destructured environment, if any.
        destructured-env (if (> (count env) 0)
                           {:keys (mapv (comp symbol name) (keys env))}
                           '?__env__)]
    `(fn ~action-name
       [~'?__token__  ~destructured-env]
       (let [~@assignments]
         ~rhs))))

(defn compile-action
  "Compile the right-hand-side action of a rule, returning a function to execute it."
  [node-id binding-keys rhs env]
  (let [;; Hardcoding the node-type and fn-type as we would only ever expect 'compile-action' to be used for this scenario
        fn-name (mk-node-fn-name "ProductionNode" node-id "AE")
        handler (compile-action-handler fn-name binding-keys rhs env)]
    `~handler))

(defn compile-accum
  "Used to create accumulators that take the environment into account."
  [node-id node-type accum env]
  (let [destructured-env
        (if (> (count env) 0)
          {:keys (mapv #(symbol (name %)) (keys env))}
          '?__env__)

        ;; AccE will stand for AccumExpr
        fn-name (mk-node-fn-name node-type node-id "AccE")]
    `(fn ~fn-name [~destructured-env]
       ~accum)))

(defn compile-join-filter
  "Compiles to a predicate function that ensures the given items can be unified. Returns a ready-to-eval
   function that accepts the following:

   * a token from the parent node
   * the fact
   * a map of bindings from the fact, which was typically computed on the alpha side
   * an environment

   The function created here returns truthy if the given fact satisfies the criteria."
  [node-id node-type {:keys [type constraints args] :as unification-condition} ancestor-bindings element-bindings env]
  (let [accessors (field-name->accessors-used type constraints)

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

        ;; Get the bindings used in the join filter expression that are pulled from
        ;; the token. This is simply the bindings in the constraints with the newly
        ;; created element bindings for this condition removed.
        token-binding-keys (remove element-bindings (variables-as-keywords constraints))

        token-assignments (mapcat build-token-assignment token-binding-keys)

        new-binding-assignments (mapcat #(list (symbol (name %))
                                               (list 'get '?__element-bindings__ %))
                                        element-bindings)

        assignments (concat
                     fact-assignments
                     token-assignments
                     new-binding-assignments)

        equality-only-variables (into #{} (for [binding ancestor-bindings]
                                            (symbol (name (keyword binding)))))

        ;; JFE will stand for JoinFilterExpr
        fn-name (mk-node-fn-name node-type node-id "JFE")]
    `(fn ~fn-name
       [~'?__token__
        ~(add-meta '?__fact__ type)
        ~'?__element-bindings__
        ~destructured-env]
       (let [~@assignments
             ~'?__bindings__ (atom {})]
         ~(compile-constraints constraints equality-only-variables)))))

(defn- expr-type [expression]
  (if (map? expression)
    :condition
    (first expression)))

(defn- cartesian-join
  "Performs a cartesian join to distribute disjunctions for disjunctive normal form.,
  This distributing each disjunction across every other disjunction and also across each
  given conjunction. Returns a sequence where each element contains a sequence
  of conjunctions that can be used in rules."
  [disjunctions-to-distribute conjunctions]

  ;; For every disjuction, do a cartesian join to distribute it
  ;; across every other disjuction. We also must distributed it across
  ;; each conjunction
  (reduce
   (fn [distributed-disjunctions disjunction-to-distribute]

     (for [expression disjunction-to-distribute
           distributed-disjunction distributed-disjunctions]
       (conj distributed-disjunction expression)))

   ;; Start with our conjunctions to join to, since we must distribute
   ;; all disjunctions across these as well.
   [conjunctions]
   disjunctions-to-distribute))

(defn to-dnf
  "Convert a lhs expression to disjunctive normal form."
  [expression]

  ;; Always validate the expression schema, as this is only done at compile time.
  (sc/validate schema/Condition expression)
  (condp = (expr-type expression)
    ;; Individual conditions can return unchanged.
    :condition
    expression

    :test
    expression

    :exists
    expression

    ;; Apply de Morgan's law to push negation nodes to the leaves.
    :not
    (let [children (rest expression)
          child (first children)]

      (when (not= 1 (count children))
        (throw (ex-info "Negation must have only one child." {:illegal-negation expression})))

      (condp = (expr-type child)

        ;; If the child is a single condition, simply return the ast.
        :condition expression

        :test expression

        ;; Note that :exists does not support further nested boolean conditions.
        ;; It is just syntax sugar over an accumulator.
        :exists expression

        ;; Double negation, so just return the expression under the second negation.
        :not
        (to-dnf (second child))

        ;; DeMorgan's law converting conjunction to negated disjuctions.
        :and (to-dnf (cons :or (for [grandchild (rest child)] [:not grandchild])))

        ;; DeMorgan's law converting disjuction to negated conjuctions.
        :or  (to-dnf (cons :and (for [grandchild (rest child)] [:not grandchild])))))

    ;; For all others, recursively process the children.
    (let [children (map to-dnf (rest expression))
          ;; Get all conjunctions, which will not conain any disjunctions since they were processed above.
          conjunctions (filter #(#{:and :condition :not :exists} (expr-type %)) children)]

      ;; If there is only one child, the and or or operator can simply be eliminated.
      (if (= 1 (count children))
        (first children)

        (condp = (expr-type expression)

          :and
          (let [disjunctions (map rest (filter #(= :or (expr-type %)) children))
                ;; Merge all child conjunctions into a single conjunction.
                combine-conjunctions (fn [children]
                                       (cons :and
                                             (for [child children
                                                   nested-child (if (= :and (expr-type child))
                                                                  (rest child)
                                                                  [child])]
                                               nested-child)))]
            (if (empty? disjunctions)
              (combine-conjunctions children)
              (cons :or
                    (for [c (cartesian-join disjunctions conjunctions)]
                      (combine-conjunctions c)))))
          :or
          ;; Merge all child disjunctions into a single disjunction.
          (let [disjunctions (mapcat rest (filter #(#{:or} (expr-type %)) children))]
            (cons :or (concat disjunctions conjunctions))))))))

(defn- non-equality-unification? [expression previously-bound]
  "Returns true if the given expression does a non-equality unification against a variable that
   is not in the previously-bound set, indicating it can't be solved by simple unification."
  (let [found-complex (atom false)
        process-form (fn [form]
                       (when (and (seq? form)
                                  (not (equality-expression? form))
                                  (some (fn [sym] (and (symbol? sym)
                                                       (.startsWith (name sym) "?")
                                                       (not (previously-bound sym))))
                                        (flatten-expression form)))

                         (reset! found-complex true))

                       form)]

    ;; Walk the expression to find use of a symbol that can't be solved by equality-based unificaiton.
    (doall (walk/postwalk process-form expression))

    @found-complex))

(defn condition-type
  "Returns the type of a single condition that has been transformed
   to disjunctive normal form. The types are: :negation, :accumulator, :test, :exists, and :join"
  [condition]
  (let [is-negation (= :not (first condition))
        is-exists (= :exists (first condition))
        accumulator (:accumulator condition)
        result-binding (:result-binding condition) ; Get the optional result binding used by accumulators.
        condition (cond
                    is-negation (second condition)
                    accumulator (:from condition)
                    :else condition)
        node-type (cond
                    is-negation :negation
                    is-exists :exists
                    accumulator :accumulator
                    (:type condition) :join
                    :else :test)]

    node-type))

(defn- extract-exists
  "Converts :exists operations into an accumulator to detect
   the presence of a fact and a test to check that count is
   greater than zero.

   It may be possible to replace this conversion with a specialized
   ExtractNode in the future, but this transformation is simple
   and meets the functional needs."
  [conditions]
  (for [condition conditions
        expanded (if (= :exists (condition-type condition))
                   ;; This is an :exists condition, so expand it
                   ;; into an accumulator and a test.
                   (let [exists-count (gensym "?__gen__")]
                     [{:accumulator '(clara.rules.accumulators/exists)
                       :from (second condition)
                       :result-binding (keyword exists-count)}])

                   ;; This is not an :exists condition, so do not change it.
                   [condition])]

    expanded))

(defn- classify-variables
  "Classifies the variables found in the given contraints into 'bound' vs 'free'
   variables.  Bound variables are those that are found in a valid
   equality-based, top-level binding form.  All other variables encountered are
   considered free.  Returns a tuple of the form
   [bound-variables free-variables]
   where bound-variables and free-variables are the sets of bound and free
   variables found in the constraints respectively."
  [constraints]
  (reduce (fn [[bound-variables free-variables] constraint]
            ;; Only top-level constraint forms can introduce new variable bindings.
            ;; If the top-level constraint is an equality expression, add the
            ;; bound variables to the set of bound variables.
            (if (and (seq? constraint) (equality-expression? constraint))
              [(->> (rest constraint)
                    (filterv is-variable?)
                    ;; A variable that was marked unbound in a previous expression should
                    ;; not be considered bound.
                    (remove free-variables)
                    (into bound-variables))
               ;; Any other variables in a nested form are now considered "free".
               (->> (rest constraint)
                    ;; We already have checked this level symbols for bound variables.
                    (remove symbol?)
                    flatten-expression
                    (filter is-variable?)
                    ;; Variables previously bound in an expression are not free.
                    (remove bound-variables)
                    (into free-variables))]

              ;; Binding forms are not supported nested within other forms, so
              ;; any variables that occur now are considered "free" variables.
              [bound-variables
               (->> (flatten-expression constraint)
                    (filterv is-variable?)
                    ;; Variables previously bound in an expression are not free.
                    (remove bound-variables)
                    (into free-variables))]))
          [#{} #{}]
          constraints))

(sc/defn analyze-condition :- {;; Variables used in the condition that are bound
                               :bound #{sc/Symbol}

                               ;; Variables used in the condition that are unbound.
                               :unbound #{sc/Symbol}

                               ;; The condition that was analyzed
                               :condition schema/Condition

                               ;; Flag indicating it is an accumulator.
                               :is-accumulator sc/Bool}
  [condition :- schema/Condition]
  (let [dnf-condition (to-dnf condition)

        ;; Break the condition up into leafs
        leaf-conditions (case (first dnf-condition)

                          ;; A top level disjunction, so get all child conjunctions and
                          ;; flatten them.
                          :or
                          (for [nested-condition (rest dnf-condition)
                                leaf-condition (if (= :and (first nested-condition))
                                                 (rest nested-condition)
                                                 [nested-condition])]
                            leaf-condition)

;; A top level and of nested conditions, so just use them
                          :and
                          (rest dnf-condition)

                          ;; The condition itself is a leaf, so keep it.
                          [dnf-condition])]

    (reduce
     (fn [{:keys [bound unbound condition is-accumulator]} leaf-condition]

;; The effective leaf for variable purposes may be contained in a negation or
       ;; an accumulator, so extract it.
       (let [effective-leaf (condp = (condition-type leaf-condition)
                              :accumulator (:from leaf-condition)
                              :negation (second leaf-condition)
                              leaf-condition)

             constraints (:constraints effective-leaf)

             [bound-variables unbound-variables] (if (#{:negation :test} (condition-type leaf-condition))
                                                   ;; Variables used in a negation should be considered
                                                   ;; unbound since they aren't usable in another condition,
                                                   ;; so label all variables as unbound.  Similarly, :test
                                                   ;; conditions can't bind new variables since they don't
                                                   ;; have any new facts as input.  See:
                                                   ;; https://github.com/cerner/clara-rules/issues/357
                                                   [#{}
                                                    (apply set/union (classify-variables constraints))]

                                                   ;; It is not a negation, so simply classify variables.
                                                   (classify-variables constraints))

             bound-with-result-bindings (cond-> bound-variables
                                          (:fact-binding effective-leaf) (conj (symbol (name (:fact-binding effective-leaf))))
                                          (:result-binding leaf-condition) (conj (symbol (name (:result-binding leaf-condition)))))

             ;; All variables bound in this condition.
             all-bound (set/union bound bound-with-result-bindings)

             ;; Unbound variables, minus those that have been bound elsewhere in this condition.
             all-unbound (set/difference (set/union unbound-variables unbound) all-bound)]

         {:bound all-bound
          :unbound all-unbound
          :condition condition
          :is-accumulator (or is-accumulator
                              (= :accumulator
                                 (condition-type leaf-condition)))}))

     {:bound #{}
      :unbound #{}
      :condition condition
      :is-accumulator false}
     leaf-conditions)))

(sc/defn sort-conditions :- [schema/Condition]
  "Performs a topologic sort of conditions to ensure variables needed by
   child conditions are bound."
  [conditions :- [schema/Condition]]
  ;; Get the bound and unbound variables for all conditions and sort them.
  (let [classified-conditions (map analyze-condition conditions)]

    (loop [sorted-conditions []
           bound-variables #{}
           remaining-conditions classified-conditions]

      (if (empty? remaining-conditions)
        ;; No more conditions to sort, so return the raw conditions
        ;; in sorted order.
        (map :condition sorted-conditions)

        ;; Unsatisfied conditions remain, so find ones we can satisfy.
        (let [satisfied? (fn [classified-condition]
                           (set/subset? (:unbound classified-condition)
                                        bound-variables))

              ;; Find non-accumulator conditions that are satisfied. We defer
              ;; accumulators until later in the rete network because they
              ;; may fire a default value if all needed bindings earlier
              ;; in the network are satisfied.
              satisfied-non-accum? (fn [classified-condition]
                                     (and (not (:is-accumulator classified-condition))
                                          (set/subset? (:unbound classified-condition)
                                                       bound-variables)))

              has-satisfied-non-accum (some satisfied-non-accum? remaining-conditions)

              newly-satisfied (if has-satisfied-non-accum
                                (filter satisfied-non-accum? remaining-conditions)
                                (filter satisfied? remaining-conditions))

              still-unsatisfied (if has-satisfied-non-accum
                                  (remove satisfied-non-accum? remaining-conditions)
                                  (remove satisfied? remaining-conditions))

              updated-bindings (apply set/union bound-variables
                                      (map :bound newly-satisfied))]

          ;; If no existing variables can be satisfied then the production is invalid.
          (when (empty? newly-satisfied)

            ;; Get the subset of variables that cannot be satisfied.
            (let [unsatisfiable (set/difference
                                 (apply set/union (map :unbound still-unsatisfied))
                                 bound-variables)]
              (throw (ex-info (str "Using variable that is not previously bound. This can happen "
                                   "when an expression uses a previously unbound variable, "
                                   "or if a variable is referenced in a nested part of a parent "
                                   "expression, such as (or (= ?my-expression my-field) ...). " \newline
                                   "Note that variables used in negations are not bound for subsequent
                                    rules since the negation can never match." \newline
                                   "Production: " \newline
                                   (:production *compile-ctx*) \newline
                                   "Unbound variables: "
                                   unsatisfiable)
                              {:production (:production *compile-ctx*)
                               :variables unsatisfiable}))))

          (recur (into sorted-conditions newly-satisfied)
                 updated-bindings
                 still-unsatisfied))))))

(defn- non-equality-unifications
  "Returns a set of unifications that do not use equality-based checks."
  [constraints]
  (let [[bound-variables unbound-variables] (classify-variables constraints)]
    (into (hs/set)
          (for [constraint constraints
                :when (non-equality-unification? constraint bound-variables)]
            constraint))))

(sc/defn condition-to-node :- schema/ConditionNode
  "Converts a condition to a node structure."
  [condition :- schema/Condition
   env :- (sc/maybe {sc/Keyword sc/Any})
   parent-bindings :- #{sc/Keyword}]
  (let [node-type (condition-type condition)
        accumulator (:accumulator condition)
        result-binding (:result-binding condition) ; Get the optional result binding used by accumulators.
        condition (cond
                    (= :negation node-type) (second condition)
                    accumulator (:from condition)
                    :else condition)

        ;; Convert a test within a negation to a negation of the test. This is necessary
        ;; because negation nodes expect an additional condition to match against.
        [node-type condition] (if (and (= node-type :negation)
                                       (= :test (condition-type condition)))

                                ;; Create a negated version of our test condition.
                                [:test {:constraints [(list 'not (cons 'and (:constraints condition)))]}]

                                ;; This was not a test within a negation, so keep the previous values.
                                [node-type condition])

        ;; Get the set of non-equality unifications that cannot be resolved locally to the rule.
        non-equality-unifications (if (or (= :accumulator node-type)
                                          (= :negation node-type)
                                          (= :join node-type))
                                    (non-equality-unifications (:constraints condition))
                                    (hs/set))

        ;; If there are any non-equality unitifications, create a join with filter expression to handle them.
        join-filter-expressions (if (seq non-equality-unifications)

                                  (assoc condition :constraints (filterv non-equality-unifications (:constraints condition)))

                                  nil)

        ;; Remove instances of non-equality constraints from accumulator
        ;; and negation nodes, since those are handled with specialized node implementations.
        condition (if (seq non-equality-unifications)
                    (assoc condition
                           :constraints (into [] (remove non-equality-unifications (:constraints condition)))
                           :original-constraints (:constraints condition))

                    condition)

        ;; Variables used in the constraints
        constraint-bindings (variables-as-keywords (:constraints condition))

        ;; Variables used in the condition.
        cond-bindings (if (:fact-binding condition)
                        (conj constraint-bindings (:fact-binding condition))
                        constraint-bindings)

        new-bindings (set/difference (variables-as-keywords (:constraints condition))
                                     parent-bindings)

        join-filter-bindings (if join-filter-expressions
                               (variables-as-keywords join-filter-expressions)
                               nil)]

    (cond-> {:node-type node-type
             :condition condition
             :new-bindings new-bindings
             :used-bindings (set/union cond-bindings join-filter-bindings)}
      (seq env) (assoc :env env)

      ;; Add the join bindings to join, accumulator or negation nodes.
      (#{:join :negation :accumulator} node-type) (assoc :join-bindings (set/intersection cond-bindings parent-bindings))

      accumulator (assoc :accumulator accumulator)

      result-binding (assoc :result-binding result-binding)

      join-filter-expressions (assoc :join-filter-expressions join-filter-expressions)

      join-filter-bindings (assoc :join-filter-join-bindings (set/intersection join-filter-bindings parent-bindings)))))

(sc/defn ^:private add-node :- schema/BetaGraph
  "Adds a node to the beta graph."
  [beta-graph :- schema/BetaGraph
   source-ids :- [sc/Int]
   target-id :- sc/Int
   target-node :- (sc/conditional
                   (comp #{:production :query} :node-type) schema/ProductionNode
                   :else schema/ConditionNode)]
  (hf/assoc! (:id-to-new-bindings beta-graph) target-id
             ;; A ProductionNode will not have the set of new bindings previously created,
             ;; so we assign them an empty set here.  A ConditionNode will always have a set of new bindings,
             ;; even if it is an empty one; this is enforced by the schema.  Having an empty set of new bindings
             ;; is a valid and meaningful state that just means that all join bindings in that node come from right-activations
             ;; earlier in the network.
             (or (:new-bindings target-node)
                 (hs/set)))
  (if (#{:production :query} (:node-type target-node))
    (hf/assoc! (:id-to-production-node beta-graph) target-id target-node)
    (hf/assoc! (:id-to-condition-node beta-graph) target-id target-node))

  ;; Add the production or condition to the network.
  ;; Associate the forward and backward edges.
  (doseq [source-id source-ids]
    (hm/compute! (:forward-edges beta-graph) source-id
                 (fn [_ forward-previous]
                   (if forward-previous
                     (conj forward-previous target-id)
                     (sorted-set target-id))))
    (hm/compute! (:backward-edges beta-graph) target-id
                 (fn [_ backward-previous]
                   (if backward-previous
                     (conj backward-previous source-id)
                     (sorted-set source-id)))))
  beta-graph)

(declare add-production)

(sc/defn ^:private get-complex-negation :- (sc/maybe
                                            {:new-expression schema/Condition
                                             :generated-rule schema/Production})
  [previous-expressions :- [schema/Condition]
   expression :- schema/Condition
   ancestor-bindings :- #{sc/Keyword}
   production :- schema/Production]
  (when (and (= :not (first expression))
             (sequential? (second expression))
             (#{:and :or :not} (first (second expression))))

    ;; Dealing with a compound negation, so extract it out.
    (let [negation-expr (second expression)
          gen-rule-name (str (or (:name production)
                                 (gensym "gen-rule"))
                             "__"
                             (gensym))

          ;; Insert the bindings from ancestors that are used in the negation
          ;; in the NegationResult fact so that the [:not [NegationResult...]]
          ;; condition can assert that the facts matching the negation
          ;; have the necessary bindings. 
          ;; See https://github.com/cerner/clara-rules/issues/304 for more details
          ;; and a case that behaves incorrectly without this check.
          ancestor-bindings-in-negation-expr (set/intersection
                                              (variables-as-keywords negation-expr)
                                              ancestor-bindings)

          ancestor-bindings-insertion-form (into {}
                                                 (map (fn [binding]
                                                        [binding (-> binding
                                                                     name
                                                                     symbol)]))
                                                 ancestor-bindings-in-negation-expr)

          ancestor-binding->restriction-form (fn [b]
                                               (list '= (-> b name symbol)
                                                     (list b 'ancestor-bindings)))

          modified-expression `[:not {:type NegationResult
                                      :constraints [(~'= ~gen-rule-name ~'gen-rule-name)
                                                    ~@(map ancestor-binding->restriction-form
                                                           ancestor-bindings-in-negation-expr)]}]

          generated-rule (cond-> {:name gen-rule-name
                                  :lhs (concat previous-expressions [negation-expr])
                                  :rhs `(clara.rules/insert! (eng/->NegationResult ~gen-rule-name
                                                                                   ~ancestor-bindings-insertion-form))}

                           ;; Propagate properties like salience to the generated production.
                           (:props production) (assoc :props (:props production))

                           true (assoc-in [:props :clara-rules/internal-salience] :extracted-negation)

                               ;; Propagate the the environment (such as local bindings) if applicable.
                           (:env production) (assoc :env (:env production)))]
      {:new-expression modified-expression
       :generated-rule generated-rule})))

(sc/defn ^:private add-complex-negation :- (sc/maybe
                                            {:new-expression schema/Condition
                                             :beta-with-negations schema/BetaGraph})

  "Extracts complex, nested negations and adds a rule to the beta graph to trigger the returned
  negation expression."
  [previous-expressions :- [schema/Condition]
   expression :- schema/Condition
   ancestor-bindings :- #{sc/Keyword}
   beta-graph :- schema/BetaGraph
   production :- schema/Production
   create-id-fn]
  (when-let [{:keys [new-expression
                     generated-rule]} (get-complex-negation previous-expressions expression
                                                            ancestor-bindings production)]
    ;; The expression was a negation, so add it to the Beta graph
    {:new-expression new-expression
     :beta-with-negations (add-production generated-rule beta-graph create-id-fn)}))

;; A beta graph with no nodes.
(defn ^:private empty-beta-graph
  []
  {:forward-edges (hf/mut-long-map)
   :backward-edges (hf/mut-long-map)
   :id-to-condition-node (hf/mut-long-map {0 ::root-condition})
   :id-to-production-node (hf/mut-long-map)
   :id-to-new-bindings (hf/mut-long-map)})

(sc/defn ^:private get-condition-bindings :- {:bindings #{sc/Keyword}}
  "Get the bindings from a sequence of condition/conjunctions."
  [conditions :- [schema/Condition]
   env :- (sc/maybe {sc/Keyword sc/Any})
   ancestor-bindings :- #{sc/Keyword}]
  (loop [bindings ancestor-bindings
         [expression & remaining-expressions] conditions]
    (if expression
      (let [node (condition-to-node expression env bindings)

            {:keys [result-binding fact-binding]} expression

            all-bindings (cond-> (set/union bindings (:used-bindings node))
                           result-binding (conj result-binding)
                           fact-binding (conj fact-binding))]
        (recur all-bindings remaining-expressions))
      ;; No expressions remaining, so return the structure.
      {:bindings bindings})))

(sc/defn ^:private add-conjunctions :- {:beta-graph schema/BetaGraph
                                        :new-ids [sc/Int]
                                        :bindings #{sc/Keyword}}

  "Adds a sequence of conjunctions to the graph in a parent-child relationship."

  [conjunctions :- [schema/Condition]
   parent-ids :- [sc/Int]
   env :- (sc/maybe {sc/Keyword sc/Any})
   ancestor-bindings :- #{sc/Keyword}
   beta-graph :- schema/BetaGraph
   create-id-fn]
  (loop [beta-graph beta-graph
         parent-ids parent-ids
         bindings ancestor-bindings
         [expression & remaining-expressions] conjunctions]

    (if expression

      (let [node (condition-to-node expression env bindings)

            {:keys [result-binding fact-binding]} expression

            all-bindings (cond-> (set/union bindings (:used-bindings node))
                           result-binding (conj result-binding)
                           fact-binding (conj fact-binding))

            ;; Find children that all parent nodes have.
            forward-edges (if (= 1 (count parent-ids))
                            ;; If there is only one parent then there is no need to reconstruct
                            ;; the set of forward edges. This is an intentional performance optimization for large
                            ;; beta graphs that have many nodes branching from a common node, typically the root node.
                            (-> (:forward-edges beta-graph)
                                (get (first parent-ids)))
                            (->> (select-keys parent-ids (:forward-edges beta-graph))
                                 vals
                                 ;; Order doesn't matter here as we will effectively sort it using update-node->id later,
                                 ;; thus adding determinism.
                                 (into (hs/set) cat)))

            id-to-condition-nodes (:id-to-condition-node beta-graph)

            ;; Since we require that id-to-condition-nodes have an equal value to "node" under the ID
            ;; for this to be used. In any possible edge cases where there are equal nodes under different IDs,
            ;; maintaining the lowest node id will add determinism.
            ;; Having different nodes under the same ID would be a bug,
            ;; but having an equivalent node under multiple IDs wouldn't necessarily be one.
            ;;
            ;; Using maps(nodes) as keys acts as a performance optimization here, we are relying on the fact that maps cache
            ;; their hash codes. This saves us time in the worst case scenarios when there are large amounts of forward edges
            ;; that will be compared repeatedly. For example, when adding new children to the root node we must first compare
            ;; all of the old children for a prior match, for each additional child added we incur the assoc but the hash code
            ;; has already been cached by prior iterations of add-conjunctions. In these cases we have seen that the hashing
            ;; will out perform equivalence checks.
            update-node->ids (fn update-node-ids [m id]
                               (hm/compute! m (get id-to-condition-nodes id)
                                            (fn do-update-node-ids
                                              [_ node-ids]
                                              (if node-ids
                                                (conj node-ids id)
                                                [id])))
                               m)

            node->ids (reduce update-node->ids
                              (hf/mut-hashtable-map)
                              forward-edges)

            backward-edges (:backward-edges beta-graph)

            parent-ids-set (set parent-ids)

            ;; Use the existing id or create a new one.
            node-id (or (when-let [common-nodes (get node->ids node)]
                          ;; We need to validate that the node we intend on sharing shares the same parents as the
                          ;; current node we are creating. See Issue 433 for more information
                          (some #(when (= (get backward-edges %)
                                          parent-ids-set)
                                   %)
                                common-nodes))
                        (create-id-fn))

            graph-with-node (add-node beta-graph parent-ids node-id node)]

        (recur graph-with-node
               [node-id]
               all-bindings
               remaining-expressions))

      ;; No expressions remaining, so return the structure.
      {:beta-graph beta-graph
       :new-ids parent-ids
       :bindings bindings})))

(sc/defn build-rule-node :- schema/ProductionNode
  [production :- schema/Production]
  (when (:rhs production)
    (let [flattened-conditions (for [condition (:lhs production)
                                     child-condition (if (#{'and :and} (first condition))
                                                       (rest condition)
                                                       [condition])]

                                 child-condition)

          sorted-conditions (sort-conditions flattened-conditions)

          {ancestor-bindings :bindings}
          (loop [previous-conditions []
                 [current-condition & remaining-conditions] sorted-conditions
                 ancestor-bindings #{}]
            (if-not current-condition
              {:bindings ancestor-bindings}
              (let [{:keys [new-expression]}
                    (get-complex-negation previous-conditions
                                          current-condition
                                          ancestor-bindings
                                          production)

                    condition (or new-expression
                                  current-condition)

                    ;; Extract disjunctions from the condition.
                    dnf-expression (to-dnf condition)

                    ;; Get a sequence of disjunctions.
                    disjunctions (for [expression
                                       (if (= :or (first dnf-expression)) ; Ignore top-level or in DNF.
                                         (rest dnf-expression)
                                         [dnf-expression])]

                                   (if (= :and (first expression)) ; Ignore nested ands in DNF.
                                     (rest expression)
                                     [expression]))

                    {all-bindings :bindings}
                    (reduce (fn [previous-result conjunctions]
                              ;; Get the  complete bindings
                              (let [;; Convert exists operations to accumulator and test nodes.
                                    exists-extracted (extract-exists conjunctions)
                                    ;; Compute the new bindings with the expressions.
                                    new-result (get-condition-bindings exists-extracted
                                                                       (:env production)
                                                                       ancestor-bindings)]

                                ;; Combine the  bindings
                                ;; for use in descendent nodes.
                                {:bindings (set/union (:bindings previous-result)
                                                      (:bindings new-result))}))

                            ;; Initial reduce value, combining previous graph, parent ids, and ancestor variable bindings.
                            {:bindings ancestor-bindings}

                            ;; Each disjunction contains a sequence of conjunctions.
                            disjunctions)]
                (recur (conj previous-conditions current-condition)
                       remaining-conditions
                       all-bindings))))]
      {:node-type :production
       :production production
       :bindings ancestor-bindings})))

(sc/defn ^:private add-production  :- schema/BetaGraph
  "Adds a production to the graph of beta nodes."
  [production :- schema/Production
   beta-graph :- schema/BetaGraph
   create-id-fn]

  ;; Flatten conditions, removing an extraneous ands so we can process them independently.
  (let [flattened-conditions (for [condition (:lhs production)
                                   child-condition (if (#{'and :and} (first condition))
                                                     (rest condition)
                                                     [condition])]

                               child-condition)

        sorted-conditions (sort-conditions flattened-conditions)]

    (loop [previous-conditions []
           [current-condition & remaining-conditions] sorted-conditions
           parent-ids [0]
           ancestor-bindings #{}
           beta-graph beta-graph]

      (if current-condition

        (let [{:keys [new-expression beta-with-negations]}
              (add-complex-negation previous-conditions
                                    current-condition
                                    ancestor-bindings
                                    beta-graph
                                    production
                                    create-id-fn)

              beta-graph (or beta-with-negations beta-graph)

              condition (or new-expression current-condition)

              ;; Extract disjunctions from the condition.
              dnf-expression (to-dnf condition)

              ;; Get a sequence of disjunctions.
              disjunctions (for [expression
                                 (if (= :or (first dnf-expression)) ; Ignore top-level or in DNF.
                                   (rest dnf-expression)
                                   [dnf-expression])]

                             (if (= :and (first expression)) ; Ignore nested ands in DNF.
                               (rest expression)
                               [expression]))

              {beta-with-nodes :beta-graph new-ids :new-ids all-bindings :bindings}
              (reduce (fn [previous-result conjunctions]

                        ;; Get the beta graph, new identifiers, and complete bindings
                        (let [;; Convert exists operations to accumulator and test nodes.
                              exists-extracted (extract-exists conjunctions)

                              ;; Compute the new beta graph, ids, and bindings with the expressions.
                              new-result (add-conjunctions exists-extracted
                                                           parent-ids
                                                           (:env production)
                                                           ancestor-bindings
                                                           (:beta-graph previous-result)
                                                           create-id-fn)]

                          ;; Combine the newly created beta graph, node ids, and bindings
                          ;; for use in descendent nodes.
                          {:beta-graph (:beta-graph new-result)
                           :new-ids (into (:new-ids previous-result) (:new-ids new-result))
                           :bindings (set/union (:bindings previous-result)
                                                (:bindings new-result))}))

                      ;; Initial reduce value, combining previous graph, parent ids, and ancestor variable bindings.
                      {:beta-graph beta-graph
                       :new-ids []
                       :bindings ancestor-bindings}

                      ;; Each disjunction contains a sequence of conjunctions.
                      disjunctions)]

          (recur (conj previous-conditions current-condition)
                 remaining-conditions
                 new-ids
                 all-bindings
                 beta-with-nodes))

        ;; No more conditions to add, so connect the production.
        (if (:rhs production)
          ;; if its a production node simply add it
          (add-node beta-graph
                    parent-ids
                    (create-id-fn)
                    {:node-type :production
                     :production production
                     :bindings ancestor-bindings})
          ;; else its a query node and we need to validate that the query has at least the bindings
          ;; specified in the parameters
          (if (every? ancestor-bindings (:params production))
            (add-node beta-graph
                      parent-ids
                      (create-id-fn)
                      {:node-type :query
                       :query production})
            (throw (ex-info "Query does not contain bindings specified in parameters."
                            {:expected-bindings (:params production)
                             :available-bindings ancestor-bindings
                             :query (:name production)}))))))))

(sc/defn to-beta-graph :- schema/BetaGraph
  "Produces a description of the beta network."
  [productions :- #{schema/Production}
   create-id-fn :- IFn]
  (reduce (fn add-to-beta-graph
            [beta-graph production]
            (binding [*compile-ctx* {:production production}]
              (add-production production beta-graph create-id-fn)))
          (empty-beta-graph)
          productions))

(sc/defn ^:private root-node? :- sc/Bool
  "A helper function to determine if the node-id provided is a root node. A given node would be considered a root-node,
   if its only backward edge was that of the artificial root node, node 0."
  [backward-edges :- schema/MutableLongHashMap
   node-id :- sc/Int]
  (= #{0} (get backward-edges node-id)))

(sc/defn extract-exprs :- schema/NodeExprLookup
  "Walks the Alpha and Beta graphs and extracts the expressions that will be used in the construction of the final network.
   The extracted expressions are stored by their key, [<node-id> <field-key>], this allows for the function to be retrieved
   after it has been compiled.

   Note: The keys of the map returned carry the metadata that can be used during evaluation. This metadata will contain,
   if available, the compile context, file and ns. This metadata is not stored on the expression itself because it will
   contain forms that will fail to eval."
  [beta-graph :- schema/BetaGraph
   alpha-graph :- [schema/AlphaNode]]
  (let [backward-edges (:backward-edges beta-graph)
        handle-expr (fn [id->expr s-expr id expr-key compilation-ctx]
                      (hf/assoc! id->expr
                                 [id expr-key]
                                 [s-expr (assoc compilation-ctx expr-key s-expr)])
                      id->expr)

        id->expr (reduce (fn add-alpha-nodes
                           [prev alpha-node]
                           (let [{:keys [id condition env]} alpha-node
                                 {:keys [type constraints fact-binding args]} condition
                                 cmeta (meta condition)]
                             (handle-expr prev
                                          (with-meta (compile-condition
                                                      type id (first args) constraints
                                                      fact-binding env)
                                                        ;; Remove all metadata but file and line number
                                                        ;; to protect from evaluating unsafe metadata
                                                        ;; See PR 243 for more detailed discussion
                                            (select-keys cmeta [:line :file]))
                                          id
                                          :alpha-expr
                                          {:file (or (:file cmeta) *file*)
                                           :compile-ctx {:condition condition
                                                         :env env
                                                         :msg "compiling alpha node"}})))
                         (hf/mut-map)
                         alpha-graph)
        id->expr (reduce-kv (fn add-action-nodes
                              [prev id production-node]
                              (let [production (-> production-node :production)]
                                (handle-expr prev
                                             (with-meta (compile-action id
                                                                        (:bindings production-node)
                                                                        (:rhs production)
                                                                        (:env production))
                                               (meta (:rhs production)))
                                             id
                                             :action-expr
                                             ;; ProductionNode expressions can be arbitrary code, therefore we need the
                                             ;; ns where the production was define so that we can compile the expression
                                             ;; later.
                                             {:ns (:ns-name production)
                                              :file (-> production :rhs meta :file)
                                              :compile-ctx {:production production
                                                            :msg "compiling production node"}})))
                            id->expr
                            (:id-to-production-node beta-graph))
        id->expr (reduce-kv
                  (fn add-conditions [prev id beta-node]
                    (let [condition (:condition beta-node)
                          condition (if (symbol? condition)
                                      (clojure.lang.RT/classForName ^String (name condition))
                                      condition)]
                      (case (or (:node-type beta-node)
                                 ;; If there is no :node-type then the node is the ::root-condition
                                 ;; however, creating a case for nil could potentially cause weird effects if something about the
                                 ;; compilation of the beta graph changes. Therefore making an explicit case for ::root-condition
                                 ;; and if there was anything that wasn't ::root-condition this case statement will fail rather than
                                 ;; failing somewhere else.
                                beta-node)
                        ::root-condition prev

                        :join (if (or (root-node? backward-edges id)
                                      (not (:join-filter-expressions beta-node)))
                                 ;; This is either a RootJoin or HashJoin node, in either case they do not have an expression
                                 ;; to capture.
                                prev
                                (handle-expr prev
                                             (compile-join-filter id
                                                                  "ExpressionJoinNode"
                                                                  (:join-filter-expressions beta-node)
                                                                  (:join-filter-join-bindings beta-node)
                                                                  (:new-bindings beta-node)
                                                                  (:env beta-node))
                                             id
                                             :join-filter-expr
                                             {:compile-ctx {:condition condition
                                                            :join-filter-expressions (:join-filter-expressions beta-node)
                                                            :env (:env beta-node)
                                                            :msg "compiling expression join node"}}))
                        :negation (if (:join-filter-expressions beta-node)
                                    (handle-expr prev
                                                 (compile-join-filter id
                                                                      "NegationWithJoinFilterNode"
                                                                      (:join-filter-expressions beta-node)
                                                                      (:join-filter-join-bindings beta-node)
                                                                      (:new-bindings beta-node)
                                                                      (:env beta-node))
                                                 id
                                                 :join-filter-expr
                                                 {:compile-ctx {:condition condition
                                                                :join-filter-expressions (:join-filter-expressions beta-node)
                                                                :env (:env beta-node)
                                                                :msg "compiling negation with join filter node"}})
                                    prev)
                        :test (handle-expr prev
                                           (compile-test id (:constraints condition) (:env beta-node))
                                           id
                                           :test-expr
                                           {:compile-ctx {:condition condition
                                                          :env (:env beta-node)
                                                          :msg "compiling test node"}})
                        :accumulator (cond-> (handle-expr prev
                                                          (compile-accum id
                                                                         (if (:join-filter-expressions beta-node)
                                                                           "AccumulateWithJoinFilterNode"
                                                                           "AccumulateNode")
                                                                         (:accumulator beta-node)
                                                                         (:env beta-node))
                                                          id
                                                          :accum-expr
                                                          {:compile-ctx {:condition condition
                                                                         :accumulator (:accumulator beta-node)
                                                                         :env (:env beta-node)
                                                                         :msg "compiling accumulator"}})

                                       (:join-filter-expressions beta-node)
                                       (handle-expr (compile-join-filter id
                                                                         "AccumulateWithJoinFilterNode"
                                                                         (:join-filter-expressions beta-node)
                                                                         (:join-filter-join-bindings beta-node)
                                                                         (:new-bindings beta-node)
                                                                         (:env beta-node))
                                                    id
                                                    :join-filter-expr
                                                    {:compile-ctx {:condition condition
                                                                   :join-filter-expressions (:join-filter-expressions beta-node)
                                                                   :env (:env beta-node)
                                                                   :msg "compiling accumulate with join filter node"}}))
                        :query prev

                         ;; This error should only be thrown if there are changes to the compilation of the beta-graph
                         ;; such as an addition of a node type.
                        (throw (ex-info "Invalid node type encountered while compiling rulebase."
                                        {:node beta-node})))))
                  id->expr
                  (:id-to-condition-node beta-graph))]
    (persistent! id->expr)))

(sc/defn compile-exprs :- schema/NodeFnLookup
  "Takes a map in the form produced by extract-exprs and evaluates the values(expressions) of the map in a batched manner.
   This allows the eval calls to be more effecient, rather than evaluating each expression on its own.
   See #381 for more details."
  [key->expr :- schema/NodeExprLookup
   expr-cache :- (sc/maybe sc/Any)
   partition-size :- sc/Int]
  (let [prepare-expr (fn do-prepare-expr
                       [[expr-key [expr compilation-ctx]]]
                       (if expr-cache
                         (let [cache-key (str (md5-hash expr) (md5-hash compilation-ctx))
                               compilation-ctx (assoc compilation-ctx :cache-key cache-key)
                               compiled-handler (some-> compilation-ctx :compile-ctx :production :handler resolve)
                               compiled-expr (or compiled-handler
                                                 (cache/lookup expr-cache cache-key))]
                           (if compiled-expr
                             [:compiled [expr-key [compiled-expr compilation-ctx]]]
                             [:prepared [expr-key [expr compilation-ctx]]]))
                         [:prepared [expr-key [expr compilation-ctx]]]))
        batching-try-eval (fn do-compile-exprs
                            [exprs compilation-ctxs]
                            ;; Try to evaluate all of the expressions as a batch. If they fail the batch eval then we
                            ;; try them one by one with their compilation context, this is slow but we were going to fail
                            ;; anyway.
                            (try
                              (mapv
                               (fn do-cache-expr
                                 [compiled-expr compilation-ctx]
                                 (when-let [cache-key (:cache-key compilation-ctx)]
                                   (cache/miss expr-cache cache-key compiled-expr))
                                 [compiled-expr compilation-ctx])
                               (eval exprs) compilation-ctxs)
                              (catch Exception e
                                ;; Using mapv here rather than map to avoid laziness, otherwise compilation failure might
                                ;; fall into the throw below for the wrong reason.
                                (mapv (fn [expr compilation-ctx]
                                        (with-bindings
                                          {#'*compile-ctx* (:compile-ctx compilation-ctx)
                                           #'*file* (:file compilation-ctx *file*)}
                                          (try-eval expr)))
                                      exprs
                                      compilation-ctxs)
                                ;; If none of the rules are the issue, it is likely that the
                                ;; size of the code trying to be evaluated has exceeded the limit
                                ;; set by java.
                                (throw (ex-info (str "There was a failure while batch evaling the node expressions, " \newline
                                                     "but wasn't present when evaling them individually. This likely indicates " \newline
                                                     "that the method size exceeded the maximum set by the jvm, see the cause for the actual error.")
                                                {:compilation-ctxs compilation-ctxs}
                                                e)))))]
    (into (hf/hash-map)
          cat
          ;; Grouping by ns, most expressions will not have a defined ns, only expressions from production nodes.
          ;; These expressions must be evaluated in the ns where they were defined, as they will likely contain code
          ;; that is namespace dependent.
          (for [[nspace ns-expr-group] (sort-by key (group-by (comp :ns second val) key->expr))
                ;; Partitioning the number of forms to be evaluated, Java has a limit to the size of methods if we were
                ;; evaluate all expressions at once it would likely exceed this limit and throw an exception.
                expr-batch (partition-all partition-size ns-expr-group) ;;;; [<node-expr-keys> [<expr> <ctx>]]
                :let [grouped-exprs (->> (mapv prepare-expr expr-batch)
                                         (group-by first))
                      prepared-exprs (map second (:prepared grouped-exprs))
                      node-expr-keys (mapv first prepared-exprs)
                      exprs (mapv (comp first second) prepared-exprs)
                      compilation-ctxs (mapv (comp second second) prepared-exprs)
                      prev-compiled-exprs (map second (:compiled grouped-exprs))
                      next-compiled-exprs (mapv vector node-expr-keys
                                                (with-bindings (if nspace
                                                                 {#'*ns* (the-ns nspace)}
                                                                 {})
                                                  (batching-try-eval exprs compilation-ctxs)))]]
            (concat prev-compiled-exprs next-compiled-exprs)))))

(defn safe-get
  "A helper function for retrieving a given key from the provided map. If the key doesn't exist within the map this
   function will throw an exception."
  [m k]
  (let [not-found ::not-found
        v (get m k not-found)]
    (if (identical? v not-found)
      (throw (ex-info "Key not found with safe-get" {:map m :key k}))
      v)))

(sc/defn ^:private compile-node
  "Compiles a given node description into a node usable in the network with the
   given children."
  [beta-node :- (sc/conditional
                 (comp #{:production :query} :node-type) schema/ProductionNode
                 :else schema/ConditionNode)
   id :- sc/Int
   is-root :- sc/Bool
   children :- [sc/Any]
   expr-fn-lookup :- schema/NodeFnLookup
   new-bindings :- #{sc/Keyword}]
  (let [{:keys [condition production query join-bindings env]} beta-node

        condition (if (symbol? condition)
                    (clojure.lang.RT/classForName ^String (name condition))
                    condition)

        compiled-expr-fn (fn [id field] (first (safe-get expr-fn-lookup [id field])))]

    (case (:node-type beta-node)

      :join
      ;; Use an specialized root node for efficiency in this case.
      (if is-root
        (eng/->RootJoinNode
         id
         condition
         children
         join-bindings)

        ;; If the join operation includes arbitrary expressions
        ;; that can't expressed as a hash join, we must use the expressions
        (if (:join-filter-expressions beta-node)
          (eng/->ExpressionJoinNode
           id
           condition
           (compiled-expr-fn id :join-filter-expr)
           children
           join-bindings)
          (eng/->HashJoinNode
           id
           condition
           children
           join-bindings)))

      :negation
      ;; Check to see if the negation includes an
      ;; expression that must be joined to the incoming token
      ;; and use the appropriate node type.
      (if (:join-filter-expressions beta-node)
        (eng/->NegationWithJoinFilterNode
         id
         condition
         (compiled-expr-fn id :join-filter-expr)
         children
         join-bindings)
        (eng/->NegationNode
         id
         condition
         children
         join-bindings))

      :test
      (eng/->TestNode
       id
       env
       (compiled-expr-fn id :test-expr)
       children)

      :accumulator
      ;; We create an accumulator that accepts the environment for the beta node
      ;; into its context, hence the function with the given environment.
      (let [compiled-node (compiled-expr-fn id :accum-expr)
            compiled-accum (compiled-node (:env beta-node))]

        ;; Ensure the compiled accumulator has the expected structure
        (when (not (instance? Accumulator compiled-accum))
          (throw (IllegalArgumentException. (str (:accumulator beta-node) " is not a valid accumulator."))))

        ;; If a non-equality unification is in place, compile the predicate and use
        ;; the specialized accumulate node.

        (if (:join-filter-expressions beta-node)
          (eng/->AccumulateWithJoinFilterNode
           id
            ;; Create an accumulator structure for use when examining the node or the tokens
            ;; it produces.
           {:accumulator (:accumulator beta-node)
             ;; Include the original filter expressions in the constraints for inspection tooling.
            :from (update-in condition [:constraints]
                             into (-> beta-node :join-filter-expressions :constraints))}
           compiled-accum
           (compiled-expr-fn id :join-filter-expr)
           (:result-binding beta-node)
           children
           join-bindings
           (:new-bindings beta-node))

          ;; All unification is based on equality, so just use the simple accumulate node.
          (eng/->AccumulateNode
           id
            ;; Create an accumulator structure for use when examining the node or the tokens
            ;; it produces.
           {:accumulator (:accumulator beta-node)
            :from condition}
           compiled-accum
           (:result-binding beta-node)
           children
           join-bindings
           (:new-bindings beta-node))))

      :production
      (eng/->ProductionNode
       id
       production
       (compiled-expr-fn id :action-expr))

      :query
      (eng/->QueryNode
       id
       query
       (:params query)))))

(sc/defn ^:private compile-beta-graph :- schema/MutableLongHashMap
  "Compile the beta description to the nodes used at runtime."
  [{:keys [id-to-production-node id-to-condition-node id-to-new-bindings forward-edges backward-edges]} :- schema/BetaGraph
   expr-fn-lookup :- schema/NodeFnLookup]
  (let [;; Sort the ids to compile based on dependencies.
        ids-to-compile (loop [pending-ids (into #{} (concat (keys id-to-production-node) (keys id-to-condition-node)))
                              node-deps forward-edges
                              sorted-nodes []]

                         (if (empty? pending-ids)
                           sorted-nodes

                           (let [newly-satisfied-ids (into #{}
                                                           (for [pending-id pending-ids
                                                                 :when (empty? (get node-deps pending-id))]
                                                             pending-id))

                                 updated-edges (into {} (for [[dependent-id dependencies]  node-deps]
                                                          [dependent-id (set/difference dependencies newly-satisfied-ids)]))]

                             (recur (set/difference pending-ids newly-satisfied-ids)
                                    updated-edges
                                    (concat sorted-nodes newly-satisfied-ids)))))
        id-to-compiled-node
        (reduce (fn [id-to-compiled-nodes id-to-compile]

                  ;; Get the condition or production node to compile
                  (let [node-to-compile (get id-to-condition-node
                                             id-to-compile
                                             (get id-to-production-node id-to-compile))

                        ;; Get the children.  The children should be sorted because the
                        ;; id-to-compiled-nodes map is sorted.
                        children (->> (get forward-edges id-to-compile)
                                      (select-keys id-to-compiled-nodes)
                                      (vals))]

                    ;; Sanity check for our logic...
                    (assert (= (count children)
                               (count (get forward-edges id-to-compile)))
                            "Each child should be compiled.")

                    (when (not= ::root-condition node-to-compile)
                      (hf/assoc! id-to-compiled-nodes
                                 id-to-compile
                                 (compile-node node-to-compile
                                               id-to-compile
                                               (root-node? backward-edges id-to-compile)
                                               children
                                               expr-fn-lookup
                                               (get id-to-new-bindings id-to-compile))))
                    id-to-compiled-nodes))
                ;; The node IDs have been determined before now, so we just need to sort the map returned.
                ;; This matters because the engine will left-activate the beta roots with the empty token
                ;; in the order that this map is seq'ed over.
                (hf/mut-long-map)
                ids-to-compile)]
    id-to-compiled-node))

(sc/defn to-alpha-graph :- [schema/AlphaNode]
  "Returns a sequence of [condition-fn, [node-ids]] tuples to represent the alpha side of the network."
  [beta-graph :- schema/BetaGraph
   create-id-fn :- IFn]
  ;; Create a sequence of tuples of conditions + env to beta node ids.
  (let [condition-to-node-ids (for [[id node] (:id-to-condition-node beta-graph)
                                    :when (:condition node)]
                                [[(:condition node) (:env node)] id])

        ;; Merge common conditions together.
        condition-to-node-map (reduce
                               (fn [node-map [[condition env] node-id]]

                                 ;; Can't use simple update-in because we need to ensure
                                 ;; the value is a vector, not a list.
                                 (hm/compute! node-map [condition env]
                                              (fn update-condition-to-node
                                                [_ node-ids]
                                                (if node-ids
                                                  (conj node-ids node-id)
                                                  [node-id])))
                                 node-map)
                               (hf/mut-map)
                               condition-to-node-ids)

        ;; We sort the alpha nodes by the ordered sequence of the node ids they correspond to
        ;; in order to make the order of alpha nodes for any given type consistent.  Note that we
        ;; coerce to a vector because we need a type that is comparable.
        condition-to-node-entries (sort-by (fn [[_k v]] (-> v sort vec))
                                           condition-to-node-map)]

    ;; Compile conditions into functions.
    (vec
     (for [[[condition env] node-ids] condition-to-node-entries
           :when (:type condition)] ; Exclude test conditions.

       (cond-> {:id (create-id-fn)
                :condition condition
                :beta-children (distinct node-ids)}
         (seq env) (assoc :env env))))))

(sc/defn compile-alpha-nodes :- [{:id sc/Int
                                  :type sc/Any
                                  :alpha-fn schema/Function
                                  (sc/optional-key :env) {sc/Keyword sc/Any}
                                  :children [sc/Num]}]
  [alpha-nodes :- [schema/AlphaNode]
   expr-fn-lookup :- schema/NodeFnLookup]
  (vec
   (for [{:keys [id condition beta-children env] :as node} alpha-nodes]
     (cond-> {:id id
              :type (effective-type (:type condition))
              :alpha-fn (first (safe-get expr-fn-lookup [id :alpha-expr]))
              :children beta-children}
       env (assoc :env env)))))

;; Wrap the fact-type so that Clojure equality and hashcode semantics are used
;; even though this is placed in a Java map.
(deftype AlphaRootsWrapper [^JavaEqualityWrapper fact-type wrapped]
  Object
  (equals [this other]
    (let [other ^AlphaRootsWrapper other]
      (.equals fact-type (.fact_type other))))

  ;; Since know we will need to find the hashcode of this object in all cases just eagerly calculate it upfront
  ;; and avoid extra calls to hash later.
  (hashCode [this] (.hash_code fact-type)))

(defn alpha-roots-wrap
  [fact-type roots]
  (AlphaRootsWrapper. (jeq-wrap fact-type) roots))

(defn- create-get-alphas-fn
  "Returns a function that given a sequence of facts,
  returns a map associating alpha nodes with the facts they accept."
  [fact-type-fn ancestors-fn alpha-roots]

  (let [;; If a customized fact-type-fn is provided,
        ;; we must use a specialized grouping function
        ;; that handles internal control types that may not
        ;; follow the provided type function.
        wrapped-fact-type-fn (if (= fact-type-fn type)
                               type
                               (fn [fact]
                                 (if (instance? ISystemFact fact)
                                   ;; Internal system types always use Clojure's type mechanism.
                                   (type fact)
                                   ;; All other types defer to the provided function.
                                   (fact-type-fn fact))))

        ;; Wrap the ancestors-fn so that we don't send internal facts such as NegationResult
        ;; to user-provided productions.  Note that this work is memoized inside fact-type->roots.
        wrapped-ancestors-fn (fn [fact-type]
                               (if (isa? fact-type ISystemFact)
                                 ;; Exclude system types from having ancestors for now
                                 ;; since none of our use-cases require them.  If this changes
                                 ;; we may need to define a custom hierarchy for them.
                                 (hs/set)
                                 (ancestors-fn fact-type)))

        fact-type->roots (memoize
                          (fn [fact-type]
                            ;; There is no inherent ordering here but we put the AlphaRootsWrapper instances
                            ;; in a vector rather than a set to avoid nondeterministic ordering (and thus nondeterministic
                            ;; performance).
                            (into []
                                  ;; If a given type in the ancestors has no matching alpha roots,
                                  ;; don't return it as an ancestor.  Fact-type->roots is memoized on the fact type,
                                  ;; but work is performed per group returned on each call the to get-alphas-fn.  Therefore
                                  ;; removing groups with no alpha nodes here will improve performance on subsequent calls
                                  ;; to the get-alphas-fn with the same fact type.
                                  (keep #(when-let [roots (not-empty (get alpha-roots %))]
                                           (alpha-roots-wrap % roots)))
                                  ;; If a user-provided ancestors-fn returns a sorted collection, for example for
                                  ;; ensuring determinism, we respect that ordering here by conj'ing on to the existing
                                  ;; collection.
                                  (conj (wrapped-ancestors-fn fact-type) fact-type))))

        update-roots->facts! (fn [^java.util.Map roots->facts roots-group fact]
                               (hm/compute! roots->facts roots-group
                                            (fn update-roots
                                              [_ facts]
                                              (let [^java.util.List fact-list (or facts (hf/mut-list))]
                                                (.add fact-list fact)
                                                fact-list))))]

    (fn [facts]
      (let [roots->facts (java.util.LinkedHashMap.)]
        (doseq [fact facts
                roots-group (fact-type->roots (wrapped-fact-type-fn fact))]
          (update-roots->facts! roots->facts roots-group fact))

        (let [return-list (hf/mut-list)
              entries (.entrySet roots->facts)
              entries-it (.iterator entries)]
          ;; We iterate over the LinkedHashMap manually to avoid potential issues described at http://dev.clojure.org/jira/browse/CLJ-1738
          ;; where a Java iterator can return the same entry object repeatedly and mutate it after each next() call.  We use mutable lists
          ;; for performance but wrap them in unmodifiableList to make it clear that the caller is not expected to mutate these lists.
          ;; Since after this function returns the only reference to the fact lists will be through the unmodifiedList we can depend elsewhere
          ;; on these lists not changing.  Since the only expected workflow with these lists is to loop through them, not add or remove elements,
          ;; we don't gain much from using a transient (which can be efficiently converted to a persistent data structure) rather than a mutable type.
          (loop []
            (when (.hasNext entries-it)
              (let [^java.util.Map$Entry e (.next entries-it)]
                (.add return-list [(-> e ^AlphaRootsWrapper (.getKey) (.wrapped))
                                   (hf/persistent! (.getValue e))])
                (recur))))
          (hf/persistent! return-list))))))

(defn create-ancestors-fn
  [{:keys [ancestors-fn
           hierarchy]}]
  (let [hierarchy-fn (when hierarchy
                       (partial ancestors hierarchy))]
    (if (and ancestors-fn hierarchy-fn)
      (comp (partial apply set/union) (juxt ancestors-fn hierarchy-fn))
      (or ancestors-fn hierarchy-fn ancestors))))

(sc/defn build-network
  "Constructs the network from compiled beta tree and condition functions."
  [id-to-node :- schema/MutableLongHashMap
   beta-roots
   alpha-fns
   productions
   fact-type-fn
   ancestors-fn
   activation-group-sort-fn
   activation-group-fn
   expr-fn-lookup]
  (let [beta-nodes (vals id-to-node)

        production-nodes (for [node beta-nodes
                               :when (= ProductionNode (type node))]
                           node)

        query-nodes (for [node beta-nodes
                          :when (= QueryNode (type node))]
                      node)

        query-map (->> (for [query-node query-nodes

                             ;; Queries can be looked up by reference or by name;
                             entry [[(:query query-node) query-node]
                                    [(:name (:query query-node)) query-node]]]
                         entry)
                       (into (hf/hash-map)))
        alpha-map (hf/mut-map)]
    ;; Merge the alpha nodes into a multi-map
    (doseq [{:keys [id type alpha-fn children env]} alpha-fns
            :let [beta-children (map id-to-node children)
                  alpha-node (eng/->AlphaNode id env beta-children alpha-fn type)]]
      (hf/assoc! id-to-node (:id alpha-node) alpha-node)
      (hm/compute! alpha-map type
                   (fn add-alpha
                     [_ alpha-nodes]
                     (if alpha-nodes
                       (conj alpha-nodes alpha-node)
                       (list alpha-node))))
      [type alpha-node])
    (let [get-alphas-fn (create-get-alphas-fn fact-type-fn ancestors-fn alpha-map)]
      #_{:clj-kondo/ignore [:unresolved-symbol]}
      (strict-map->Rulebase
       {:alpha-roots (hf/persistent! alpha-map)
        :beta-roots beta-roots
        :productions productions
        :production-nodes production-nodes
        :query-nodes query-map
        :id-to-node (hf/persistent! id-to-node)
        :activation-group-sort-fn activation-group-sort-fn
        :activation-group-fn activation-group-fn
        :get-alphas-fn get-alphas-fn
        :node-expr-fn-lookup expr-fn-lookup}))))

(defn production-load-order-comp [a b]
  (< (-> a meta ::rule-load-order)
     (-> b meta ::rule-load-order)))

(defn validate-names-unique
  "Checks that all productions included in the session have unique names,
   throwing an exception if duplicates are found."
  [productions]
  (let [non-unique (->> productions
                        (group-by :name)
                        (filter (fn [[k v]] (and (some? k) (not= 1 (count v)))))
                        (map key)
                        set)]
    (if (empty? non-unique)
      productions
      (throw (ex-info (str "Non-unique production names: " non-unique) {:names non-unique})))))

(def forms-per-eval-default
  "The default max number of forms that will be evaluated together as a single batch.
   5000 is chosen here due to the way that clojure will evaluate the vector of forms extracted from the nodes.
   The limiting factor here is the max java method size (64KiB), clojure will compile each form in the vector down into
   its own class file and generate another class file that will reference each of the other functions and wrap them in
   a vector inside a static method. For example,

   (eval [(fn one [_] ...) (fn two [_] ...)])
   would generate 3 classes.

   some_namespace$eval1234
   some_namespace$eval1234$one_1234
   some_namespace$eval1234$two_1235

   some_namespace$eval1234$one_1234 and some_namespace$eval1234$two_1235 contian the implementation of the functions,
   where some_namespace$eval1234 will contain two methods, invoke and invokeStatic.
   The invokeStatic method breaks down into something similar to a single create array call followed by 2 array set calls
   with new invocations on the 2 classes the method then returns a new vector created from the array.

   5000 is lower than the absolute max to allow for modifications to how clojure compiles without needing to modify this.
   The current limit should be 5471, this is derived from the following opcode investigation:

   Array creation:                                                    5B
   Creating and populating the first 6 elements of the array:        60B
   Creating and populating the next 122 elements of the array:    1,342B
   Creating and populating the next 5343 elements of the array:  64,116B
   Creating the vector and the return statement:                      4B

   This sums to 65,527B just shy of the 65,536B method size limit."
  5000)

(def omit-compile-ctx-default
  "During construction of the Session there is data maintained such that if the underlying expressions fail to compile
   then this data can be used to explain the failure and the constraints of the rule who's expression is being evaluated.
   The default behavior will be to discard this data, as there will be no use unless the session will be serialized and
   deserialized into a dissimilar environment, ie function or symbols might be unresolvable. In those sorts of scenarios
   it would be possible to construct the original Session with the `omit-compile-ctx` flag set to false, then the compile
   context should aid in debugging the compilation failure on deserialization."
  true)

(sc/defn mk-session*
  "Compile the rules into a rete network and return the given session."
  [productions :- #{schema/Production}
   facts :- [sc/Any]
   options :- {sc/Keyword sc/Any}]
  (validate-names-unique productions)
  (let [;; A stateful counter used for unique ids of the nodes of the graph.
        id-counter (atom 0)
        create-id-fn (fn [] (swap! id-counter inc))

        compiler-cache (:compiler-cache options default-compiler-cache)
        forms-per-eval (:forms-per-eval options forms-per-eval-default)

        beta-graph (to-beta-graph productions create-id-fn)
        alpha-graph (to-alpha-graph beta-graph create-id-fn)

        ;; Extract the expressions from the graphs and evaluate them in a batch manner.
        ;; This is a performance optimization, see Issue 381 for more information.
        exprs (compile-exprs (extract-exprs beta-graph alpha-graph) compiler-cache forms-per-eval)

        ;; If we have made it to this point, it means that we have succeeded in compiling all expressions
        ;; thus we can free the :compile-ctx used for troubleshooting compilation failures.
        ;; The reason that this flag exists is in the event that this session will be serialized with an
        ;; uncertain deserialization environment and this sort of troubleshooting information would be useful
        ;; in diagnosing compilation errors in specific rules.
        omit-compile-ctx (:omit-compile-ctx options omit-compile-ctx-default)
        exprs (if omit-compile-ctx
                (into (hf/hash-map)
                      (map
                       (fn [[k [expr ctx]]]
                         [k [expr (dissoc ctx :compile-ctx)]]))
                      exprs)
                exprs)

        beta-tree (compile-beta-graph beta-graph exprs)
        beta-root-ids (-> beta-graph :forward-edges (get 0)) ; 0 is the id of the virtual root node.
        beta-roots (vals (select-keys beta-tree beta-root-ids))
        alpha-nodes (compile-alpha-nodes alpha-graph exprs)

        ;; The fact-type uses Clojure's type function unless overridden.
        fact-type-fn (or (get options :fact-type-fn)
                         type)

        ;; The ancestors for a logical type uses Clojure's ancestors function unless overridden.
        ancestors-fn (create-ancestors-fn options)

        ;; The default is to sort activations in descending order by their salience.
        activation-group-sort-fn (eng/options->activation-group-sort-fn options)

        ;; The returned salience will be a tuple of the form [rule-salience internal-salience],
        ;; where internal-salience is considered after the rule-salience and is assigned automatically by the compiler.
        activation-group-fn (eng/options->activation-group-fn options)

        rulebase (build-network beta-tree beta-roots alpha-nodes productions
                                fact-type-fn ancestors-fn activation-group-sort-fn activation-group-fn
                                exprs)

        get-alphas-fn (:get-alphas-fn rulebase)

        transport (LocalTransport.)

        session (eng/assemble {:rulebase rulebase
                               :memory (eng/local-memory rulebase transport activation-group-sort-fn activation-group-fn get-alphas-fn)
                               :transport transport
                               :listeners (get options :listeners  [])
                               :get-alphas-fn get-alphas-fn})]
    (eng/insert session facts)))

(defn add-production-load-order
  "Adds ::rule-load-order to metadata of productions. Custom DSL's may need to use this if
   creating a session in Clojure without calling mk-session below."
  [productions]
  (map (fn [n production]
         (vary-meta production assoc ::rule-load-order (or n 0)))
       (range) productions))

(defn load-rules-from-source
  "loads the rules from a source if it implements `IRuleSource`, or navigates inside
  collections to load rules from vectors, lists, sets, seqs."
  [source]
  (cond
    (u/instance-satisfies? IRuleSource source)
    (load-rules source)

    (or (vector? source)
        (list? source)
        (set? source)
        (seq? source))
    (mapcat load-rules-from-source source)

    (var? source)
    (load-rules-from-source @source)

    (:lhs source)
    [source]

    :else []))

(defn load-facts-from-source
  "loads the hierarchies from a source if it implements `IRuleSource`, or navigates inside
  collections to load from vectors, lists, sets, seqs."
  [source]
  (cond
    (u/instance-satisfies? IFactSource source)
    (load-facts source)

    (or (vector? source)
        (list? source)
        (set? source)
        (seq? source))
    (mapcat load-facts-from-source source)

    (var? source)
    (load-facts-from-source @source)

    (fn? source) ;;; source is a rule fn so it can't also be a fact unless explicitly inserted
    []

    (:hierarchy-data source) ;;; source is a hierarchy so it can't also be a fact unless explicitly inserted
    []

    (:lhs source) ;;; source is a production so it can't also be a fact unless explicitly inserted
    []

    :else [source]))

(defn load-hierarchies-from-source
  "loads the hierarchies from a source if it implements `IRuleSource`, or navigates inside
  collections to load from vectors, lists, sets, seqs."
  [source]
  (cond
    (u/instance-satisfies? IHierarchySource source)
    (load-hierarchies source)

    (or (vector? source)
        (list? source)
        (set? source)
        (seq? source))
    (mapcat load-hierarchies-from-source source)

    (var? source)
    (load-hierarchies-from-source @source)

    (:hierarchy-data source)
    [source]

    :else []))

(defn- reduce-hierarchy
  [h {:keys [hierarchy-data]}]
  (reduce (fn apply-op
            [h [op tag parent]]
            (case op
              :d (hierarchy/derive h tag parent)
              :u (hierarchy/underive h tag parent)
              (throw (ex-info "Unsupported operation building hierarchy" {:op op})))) h hierarchy-data))

(defn mk-session
  "Creates a new session using the given rule source. The resulting session
  is immutable, and can be used with insert, retract, fire-rules, and query functions."
  ([sources-and-options]
   (let [sources (take-while (complement keyword?) sources-and-options)
         options (apply hash-map (drop-while (complement keyword?) sources-and-options))
         productions-loaded (->> (mapcat load-rules-from-source sources)
                                 (add-production-load-order))
         productions-unique (hs/set productions-loaded)
         productions-sorted (with-meta
                              (into (sorted-set-by production-load-order-comp) productions-unique)
                              ;; Store the name of the custom comparator for durability.
                              {:clara.rules.durability/comparator-name `production-load-order-comp})
         hierarchies-loaded (cond->> (mapcat load-hierarchies-from-source sources)
                              (:hierarchy options) (cons (:hierarchy options)))
         hierarchy (when (seq hierarchies-loaded)
                     (reduce reduce-hierarchy (hierarchy/make-hierarchy) hierarchies-loaded))
         facts (->> (mapcat load-facts-from-source sources)
                    (vec))
         options (cond-> options
                   (some? hierarchy)
                   (assoc :hierarchy hierarchy))
         options-cache (get options :cache)
         session-cache (cond
                         (true? options-cache)
                         default-session-cache
                         (nil? options-cache)
                         default-session-cache
                         :else options-cache)
         ;;; this is simpler than storing all the productions and options in the cache
         session-key (str (md5-hash productions-sorted)
                          (md5-hash (dissoc options :cache :compiler-cache))
                          (hash facts))]
     (if session-cache
       (cache/lookup-or-miss session-cache session-key
                             (fn do-mk-session [_]
                               (mk-session* productions-sorted facts options)))
       (mk-session* productions-sorted facts options)))))
