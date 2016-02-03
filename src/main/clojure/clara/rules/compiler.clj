(ns clara.rules.compiler
  "This namespace is for internal use and may move in the future.
   This is the Clara rules compiler, translating raw data structures into compiled versions and functions.
   Most users should use only the clara.rules namespace."
  (:require [clara.rules.engine :as eng]
            [clara.rules.listener :as listener]
            [clara.rules.platform :as platform]
            [clara.rules.schema :as schema]
            [clojure.core.reducers :as r]
            [clojure.reflect :as reflect]
            [clojure.set :as s]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [schema.core :as sc]
            [schema.macros :as sm])

  (:import [clara.rules.engine ProductionNode QueryNode HashJoinNode ExpressionJoinNode
            NegationNode TestNode AccumulateNode AlphaNode LocalTransport
            LocalSession Accumulator]
           [java.beans PropertyDescriptor]))

;; Protocol for loading rules from some arbitrary source.
(defprotocol IRuleSource
  (load-rules [source]))

;; These nodes exist in the beta network.
(def BetaNode (sc/either ProductionNode QueryNode HashJoinNode ExpressionJoinNode
                         NegationNode TestNode AccumulateNode))

;; A rulebase -- essentially an immutable Rete network with a collection of alpha and beta nodes and supporting structure.
(sc/defrecord Rulebase [;; Map of matched type to the alpha nodes that handle them.
                        alpha-roots :- {sc/Any [AlphaNode]}
                        ;; Root beta nodes (join, accumulate, etc.)
                        beta-roots :- [BetaNode]
                        ;; Productions in the rulebase.
                        productions :- #{schema/Production}
                        ;; Production nodes.
                        production-nodes :- [ProductionNode]
                        ;; Map of queries to the nodes hosting them.
                        query-nodes :- {sc/Any QueryNode}
                        ;; May of id to one of the beta nodes (join, accumulate, etc)
                        id-to-node :- {sc/Num BetaNode}])

(def ^:private reflector
  "For some reason (bug?) the default reflector doesn't use the
   Clojure dynamic class loader, which prevents reflecting on
  `defrecords`.  Work around by supplying our own which does."
  (clojure.reflect.JavaReflector. (clojure.lang.RT/makeClassLoader)))

;; This technique borrowed from Prismatic's schema library.
(defn compiling-cljs?
  "Return true if we are currently generating cljs code.  Useful because cljx does not
         provide a hook for conditional macro expansion."
  []
  (boolean
   (when-let [n (find-ns 'cljs.analyzer)]
     (when-let [v (ns-resolve n '*cljs-file*)]

       ;; We perform this require only if we are compiling ClojureScript
       ;; so non-ClojureScript users do not need to pull in
       ;; that dependency.
       (require 'clara.macros)
       @v))))

(defn get-namespace-info
  "Get metadata about the given namespace."
  [namespace]
  (when-let [n (and (compiling-cljs?) (find-ns 'cljs.env))]
    (when-let [v (ns-resolve n '*compiler*)]
      (get-in @@v [ :cljs.analyzer/namespaces namespace]))))

(defn cljs-ns
  "Returns the ClojureScript namespace being compiled during Clojurescript compilation."
  []
  (if (compiling-cljs?)
    (-> 'cljs.analyzer (find-ns) (ns-resolve '*cljs-ns*) deref)
    nil))

(defn resolve-cljs-sym
  "Resolves a ClojureScript symbol in the given namespace."
  [ns-sym sym]
  (let [ns-info (get-namespace-info ns-sym)]
    (if (namespace sym)

      ;; Symbol qualified by a namespace, so look it up in the requires info.
      (if-let [source-ns (get-in ns-info [:requires (symbol (namespace sym))])]
        (symbol (name source-ns) (name sym))
        ;; Not in the requires block, so assume the qualified name is a refers and simply return the symbol.
        sym)

      ;; Symbol is unqualified, so check in the uses block.
      (if-let [source-ns (get-in ns-info [:uses sym])]
        (symbol (name source-ns) (name sym))

        ;; Symbol not found in eiher block, so attempt to retrieve it from
        ;; the current namespace.
        (if (get-in (get-namespace-info ns-sym) [:defs sym])
          (symbol (name ns-sym) (name sym))
          nil)))))

(defn- get-cljs-accessors
  "Returns accessors for ClojureScript. WARNING: this touches
  ClojureScript implementation details that may change."
  [sym]
  (let [resolved (resolve-cljs-sym (cljs-ns) sym)
        constructor (symbol (str "->" (name resolved)))
        namespace-info (get-namespace-info (symbol (namespace resolved)))
        constructor-info (get-in namespace-info [:defs constructor])]

    (if constructor-info
      (into {}
            (for [field (first (:method-params constructor-info))]
              [field (keyword (name field))]))
      [])))


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
                                                    (getPropertyDescriptors)))]

          [(symbol (string/replace (.. property (getName)) #"_" "-")) ; Replace underscore with idiomatic dash.
           (symbol (str "." (.. property (getReadMethod) (getName))))])))

(defn effective-type [type]
  (if (compiling-cljs?)
    type

    (if (symbol? type)
      (.loadClass (clojure.lang.RT/makeClassLoader) (name type))
      type)))

(defn get-fields
  "Returns a map of field name to a symbol representing the function used to access it."
  [type]
  (if (compiling-cljs?)

    ;; Get ClojureScript fields.
    (if (symbol? type)
      (get-cljs-accessors type)
      [])

    ;; Attempt to load the corresponding class for the type if it's a symbol.
    (let [type (effective-type type)]

      (cond
       (isa? type clojure.lang.IRecord) (get-field-accessors type)
       (class? type) (get-bean-accessors type) ; Treat unrecognized classes as beans.
       :default []))))

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

(defn- try-eval
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
     `((deref ~'?__bindings__))
     (let [ [exp & rest-exp] exp-seq
            compiled-rest (compile-constraints rest-exp equality-only-variables)
            variables (into #{}
                            (filter (fn [item]
                                      (and (symbol? item)
                                           (= \? (first (name item)))
                                           (not (equality-only-variables item))))
                                    exp))
            expression-values (remove variables (rest exp))
            binds-variables? (and (equality-expression? exp)
                                  (seq variables))]
       (when (and binds-variables?
                  (empty? expression-values))

         (throw (ex-info (str "Malformed variable binding for " variables ". No associated value.")
                         {:variables (map keyword variables)})))

       (if binds-variables?

         (concat

          ;; Bind each variable with the first value we encounter.
          ;; The additional equality checks are handled below so which value
          ;; we bind to is not important. So an expression like (= ?x value-1 value-2) will
          ;; bind ?x to value-1, and then ensure value-1 and value-2 are equal below.
          (for [variable variables]
            `(swap! ~'?__bindings__ assoc ~(keyword variable) ~(first expression-values)))

          ;; If there is more than one expression value, we need to ensure they are
          ;; equal as well as doing the bind. This ensures that value-1 and value-2 are
          ;; equal.
          (if (> (count expression-values) 1)


            (list (list 'if (cons '= expression-values) (cons 'do compiled-rest) nil))
            ;; No additional values to check, so move on to the rest of
            ;; the expression
            compiled-rest))

         ;; No variables to unify, so simply check the expression and
         ;; move on to the rest.
         (list (list 'if exp (cons 'do compiled-rest) nil)))))))

(defn flatten-expression
  "Flattens expression as clojure.core/flatten does, except will flatten
   anything that is a collection rather than specifically sequential."
  [expression]
  (filter (complement coll?)
          (tree-seq coll? seq expression)))

(defn variables-as-keywords
  "Returns symbols in the given s-expression that start with '?' as keywords"
  [expression]
  (into #{} (for [item (flatten-expression expression)
                  :when (and (symbol? item)
                             (= \? (first (name item))))]
              (keyword  item))))

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

(defn compile-condition
  "Returns a function definition that can be used in alpha nodes to test the condition."
  [type destructured-fact constraints result-binding env]
  (let [;; Get a map of fieldnames to access function symbols.
        accessors (get-fields type)

        binding-keys (variables-as-keywords constraints)
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

    `(fn [~(add-meta '?__fact__ type)
          ~destructured-env] ;; TODO: add destructured environment parameter...
       (let [~@assignments
             ~'?__bindings__ (atom ~initial-bindings)]
         (do ~@(compile-constraints constraints))))))

;; FIXME: add env...
(defn compile-test [tests]
  (let [binding-keys (variables-as-keywords tests)
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
  [{:keys [type constraints args] :as unification-condition} ancestor-bindings env]
  (let [accessors (get-fields type)

        binding-keys (variables-as-keywords constraints)

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
                     token-assignments)

        equality-only-variables (into #{} (for [binding ancestor-bindings]
                                            (symbol (name (keyword binding)))))]

    `(fn [~'?__token__
         ~(add-meta '?__fact__ type)
          ~destructured-env]
       (let [~@assignments
             ~'?__bindings__ (atom {})]
         (do ~@(compile-constraints constraints equality-only-variables))))))

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
        (throw (RuntimeException. "Negation must have only one child.")))

      (condp = (expr-type child)

        ;; If the child is a single condition, simply return the ast.
        :condition expression

        :test expression

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
          conjunctions (filter #(#{:and :condition :not} (expr-type %)) children)]

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

(defn- non-equality-unification? [expression]
  "Returns true if the given expression does a non-equality unification against a variable,
   indicating it can't be solved by simple unification."
  (let [found-complex (atom false)
        process-form (fn [form]
                       (when (and (seq? form)
                                  (not (equality-expression? form))
                                  (some (fn [sym] (and (symbol? sym)
                                                      (.startsWith (name sym) "?")
                                                      (not= sym '?__fact__)))
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
        is-query (:query condition)
        result-binding (:result-binding condition) ; Get the optional result binding used by accumulators.
        condition (cond
                   is-negation (second condition)
                   accumulator (:from condition)
                   :default condition)
        node-type (cond
                   is-negation :negation
                   is-exists :exists
                   accumulator :accumulator
                   is-query :query
                   (:type condition) :join
                   :else :test)]

    node-type))

(defn- is-variable?
  "Returns true if the given expression is a variable (a symbol prefixed by ?)"
  [expr]
  (and (symbol? expr)
       (.startsWith (name expr) "?")))

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
                       [{:accumulator '(clara.rules.accumulators/count)
                         :from (second condition)
                         :result-binding (keyword exists-count)}
                        {:constraints [(list '> exists-count 0)]}])

                   ;; This is not an :exists condition, so do not change it.
                   [condition])]

    expanded))

(defn- query-result-type
  "Returns the type used to insert query results in memory so they can be matched in conditions."
  [query env]

  (let [query-name (cond
                     (map? query) (:name query)

                     (symbol? query) (or (->> (keyword query)
                                              (get env)
                                              :name)

                                         (when-let [query-var (resolve query)]
                                           (:name (deref query-var)))

                                         (throw (ex-info (str "Unable to resolve query: " query)
                                                         {:query query})))

                     (string? query) query

                     :default
                     (throw (ex-info (str "Invalid query: " query)
                                     {:query query})))

        qualified-name (if (namespace (keyword query-name))
                         query-name
                         (str "clara.query.default/" query-name))

        query-type (keyword qualified-name)]

    ;; Derive the new query type from the result.
    (derive query-type :clara.rules.engine/query-result)

    query-type))

(defn- convert-query-condition
  "Converts a query condition into a join condition of the query result type."
  [condition env]
  (if (= :query (condition-type condition))

    (let [{:keys [query params constraints args fact-binding]} condition
            join-type (query-result-type query env)

            ;; Constraints used to match the query. Users may add arbitrarty
            ;; constraints as well.
            query-constraints (for [[name value] params]
                                (list '= value (list 'get '?__fact__ name)))]

        (cond-> {:type join-type
                 :constraints (concat query-constraints constraints)}
          args (assoc :args args)
          fact-binding (assoc :fact-binding fact-binding)))

    ;; The condition is not a query, so simply return it.
    condition))

(defn- convert-query-nodes [conditions env]
  "Converts query operations to join nodes of query results."
  (for [condition conditions]

    (case (condition-type condition)

      ;; Test and join nodes are leafs that cannot contain queries.
      :test condition

      :join condition

      :query (convert-query-condition condition env)

      :accumulator (assoc condition :from (convert-query-condition (:from condition) env))

      :negation [:not (convert-query-condition (second condition) env)])))

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
                    (into bound-variables))
               ;; Any other variables in a nested form are now considered "free".
               (->> (rest constraint)
                    ;; We already have checked this level symbols for bound variables.
                    (remove symbol?)
                    flatten-expression
                    (filterv is-variable?)
                    (into free-variables))]

              ;; Binding forms are not supported nested within other forms, so
              ;; any variables that occur now are considered "free" variables.
              [bound-variables
               (->> (flatten-expression constraint)
                    (filterv is-variable?)
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

             [bound-variables unbound-variables] (if (= :negation (condition-type leaf-condition))
                                                   ;; Variables used in a negation should be considered
                                                   ;; unbound since they aren't usable in another condition,
                                                   ;; so label all variables as unbound.
                                                   [#{}
                                                    (apply s/union (classify-variables constraints))]

                                                   ;; It is not a negation, so simply classify variables.
                                                   (classify-variables constraints))

             bound-with-result-bindings (cond-> bound-variables
                                          (:fact-binding effective-leaf) (conj (symbol (name (:fact-binding effective-leaf))))
                                          (:result-binding leaf-condition) (conj (symbol (name (:result-binding leaf-condition)))))

             ;; All variables bound in this condition.
             all-bound (s/union bound bound-with-result-bindings)

             ;; Unbound variables, minus those that have been bound elsewhere in this condition.
             all-unbound (s/difference (s/union unbound-variables unbound) all-bound)]

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
                           (clojure.set/subset? (:unbound classified-condition)
                                                bound-variables))

              ;; Find non-accumulator conditions that are satisfied. We defer
              ;; accumulators until later in the rete network because they
              ;; may fire a default value if all needed bindings earlier
              ;; in the network are satisfied.
              satisfied-non-accum? (fn [classified-condition]
                                     (and (not (:is-accumulator classified-condition))
                                          (clojure.set/subset? (:unbound classified-condition)
                                                               bound-variables)))

              has-satisfied-non-accum (some satisfied-non-accum? remaining-conditions)

              newly-satisfied (if has-satisfied-non-accum
                                (filter satisfied-non-accum? remaining-conditions)
                                (filter satisfied? remaining-conditions))

              still-unsatisfied (if has-satisfied-non-accum
                                  (remove satisfied-non-accum? remaining-conditions)
                                  (remove satisfied? remaining-conditions))

              updated-bindings (apply clojure.set/union bound-variables
                                      (map :bound newly-satisfied))]

          ;; If no existing variables can be satisfied then the production is invalid.
          (when (empty? newly-satisfied)

            ;; Get the subset of variables that cannot be satisfied.
            (let [unsatisfiable (clojure.set/difference
                                 (apply clojure.set/union (map :unbound still-unsatisfied))
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
                   :default condition)

        ;; Convert a test within a negation to a negation of the test. This is necessary
        ;; because negation nodes expect an additional condition to match against.
        [node-type condition] (if (and (= node-type :negation)
                                       (= :test (condition-type condition)))

                                ;; Create a negated version of our test condition.
                                [:test {:constraints [(list 'not (cons 'and (:constraints condition)))]}]

                                ;; This was not a test within a negation, so keep the previous values.
                                [node-type condition])

        ;; Get the non-equality unifications so we can handle them
        join-filter-expressions (if (and (or (= :accumulator node-type)
                                             (= :negation node-type)
                                             (= :join node-type))
                                         (some non-equality-unification? (:constraints condition)))

                                    (assoc condition :constraints (filterv non-equality-unification? (:constraints condition)))

                                    nil)

        ;; Remove instances of non-equality constraints from accumulator
        ;; and negation nodes, since those are handled with specialized node implementations.
        condition (if (and (or (= :accumulator node-type)
                               (= :negation node-type)
                               (= :join node-type))
                           (some non-equality-unification? (:constraints condition)))

                    (assoc condition
                      :constraints (into [] (remove non-equality-unification? (:constraints condition)))
                      :original-constraints (:constraints condition))

                    condition)

        ;; Variables used in the constraints
        constraint-bindings (variables-as-keywords (:constraints condition))

        ;; Variables used in the condition.
        cond-bindings (if (:fact-binding condition)
                        (conj constraint-bindings (:fact-binding condition))
                        constraint-bindings)

        join-filter-bindings (if join-filter-expressions
                               (variables-as-keywords join-filter-expressions)
                               nil)]

        (cond->
            {:node-type node-type
             :condition condition
             :used-bindings (s/union cond-bindings join-filter-bindings)}

          (seq env) (assoc :env env)

          ;; Add the join bindings to join, accumulator or negation nodes.
          (#{:join :negation :accumulator} node-type) (assoc :join-bindings (s/intersection cond-bindings parent-bindings))

          accumulator (assoc :accumulator accumulator)

          result-binding (assoc :result-binding result-binding)

          join-filter-expressions (assoc :join-filter-expressions join-filter-expressions)

          join-filter-bindings (assoc :join-filter-join-bindings (s/intersection join-filter-bindings parent-bindings)))))

(sc/defn ^:private add-node :- schema/BetaGraph
  "Adds a node to the beta graph."
  [beta-graph :- schema/BetaGraph
   source-ids :- [sc/Int]
   target-id :- sc/Int
   target-node :- (sc/either schema/ConditionNode schema/ProductionNode)]

  ;; Add the production or condition to the network.
  (let [beta-graph (if (#{:production :query} (:node-type target-node))
                          (assoc-in beta-graph [:id-to-production-node target-id] target-node)
                          (assoc-in beta-graph [:id-to-condition-node target-id] target-node))]


    ;; Associate the forward and backward edges.
    (reduce (fn [beta-graph source-id]
              (let [forward-path [:forward-edges source-id]
                    forward-previous (get-in beta-graph forward-path)
                    backward-path [:backward-edges target-id]
                    backward-previous (get-in beta-graph backward-path)]
                (-> beta-graph
                    (assoc-in
                     forward-path
                     (if forward-previous
                       (conj forward-previous target-id)
                       #{target-id}))
                    (assoc-in
                     backward-path
                     (if backward-previous
                       (conj backward-previous source-id)
                       #{source-id})))))

            beta-graph
            source-ids)))


(declare add-production)

(sc/defn ^:private extract-negation :-  {:new-expression schema/Condition
                                         :beta-with-negations schema/BetaGraph}

  "Extracts complex, nested negations and adds a rule to the beta graph to trigger the returned
   negation expression."
  [previous-expressions :- [schema/Condition]
   expression :- schema/Condition
   parent-ids :- [sc/Int]
   ancestor-bindings :- #{sc/Keyword}
   beta-graph :- schema/BetaGraph
   production :- schema/Production
   create-id-fn]

  (if (and (= :not (first expression))
           (sequential? (second expression))
           (#{:and :or :not} (first (second expression))))

    ;; Dealing with a compound negation, so extract it out.
    (let [negation-expr (second expression)
          gen-rule-name (str (or (:name production)
                                 (gensym "gen-rule"))
                             "__"
                             (gensym))

          modified-expression `[:not {:type ~(if (compiling-cljs?)
                                               'clara.rules.engine/NegationResult
                                               'clara.rules.engine.NegationResult)
                                      :constraints [(~'= ~gen-rule-name ~'gen-rule-name)]}]



          generated-rule (cond-> {:name gen-rule-name
                                  :lhs (concat previous-expressions [negation-expr])
                                  :rhs `(clara.rules/insert! (eng/->NegationResult ~gen-rule-name))}

                           ;; Propagate properties like salience to the generated production.
                           (:props production) (assoc :props (:props production))

                               ;; Propagate the the environment (such as local bindings) if applicable.
                           (:env production) (assoc :env (:env production)))

          ;; Add the generated rule to the beta network.


          beta-with-negations (add-production generated-rule beta-graph create-id-fn)]

      {:new-expression modified-expression
       :beta-with-negations beta-with-negations})

    ;; The expression wasn't a negation, so return the previous content.
    {:new-expression expression
     :beta-with-negations beta-graph}))

;; A beta graph with no nodes.
(def ^:private empty-beta-graph {:forward-edges {}
                                 :backward-edges {}
                                 :id-to-condition-node {0 ::root-condition}
                                 :id-to-production-node {}})

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

            all-bindings (cond-> (s/union bindings (:used-bindings node))
                           result-binding (conj result-binding)
                           fact-binding (conj fact-binding))

            ;; Find children that all parent nodes have.
            common-children-ids (apply s/union (-> (:forward-edges beta-graph)
                                                   (select-keys parent-ids)
                                                   (vals)))


            id-to-condition-nodes (:id-to-condition-node beta-graph)

            ;; Find an id for an equivalent node.
            existing-id (first (for [child-id common-children-ids
                                     :when (= node (get id-to-condition-nodes child-id))]
                                 child-id))

            ;; Use the existing id or create a new one.
            node-id (or existing-id
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
              (extract-negation previous-conditions
                                current-condition
                                parent-ids
                                ancestor-bindings
                                beta-graph
                                production
                                create-id-fn)

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

              {beta-with-nodes :beta-graph new-ids :new-ids all-bindings :bindings}
              (reduce (fn [previous-result conjunctions]

                        ;; Get the beta graph, new identifiers, and complete bindings
                        (let [;; Convert exists operations to accumulator and test nodes.
                              exists-extracted (extract-exists conjunctions)

                              ;; Convert queries to join nodes.
                              queries-converted (convert-query-nodes exists-extracted (:env production))

                              ;; Compute the new beta graph, ids, and bindings with the expressions.
                              new-result (add-conjunctions queries-converted
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
                                                (:bindings new-result)) }
                          ))

                      ;; Initial reduce value, combining previous graph, parent ids, and ancestor variable bindings.
                      {:beta-graph beta-with-negations
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
        (add-node beta-graph
                  parent-ids
                  (create-id-fn)
                  (if (:rhs production)
                    {:node-type :production
                     :production production
                     :bindings ancestor-bindings}
                    {:node-type :query
                     :query production}))))))

(sc/defn ^:private wire-queries :- schema/BetaGraph
  "Discover queries that are invoked by the left-hand side of other rules
   and wire them into the graph. This is done by generating a rule node
   that is a sibling to the query and inserting the query results into the graph
   with a type tag that matches that specific query."
  [beta-graph :- schema/BetaGraph
   create-id-fn]

  (let [;; Map of query name to type expected byt he rule.
        query-name-to-type (into {}
                                 (for [query-condition-node (vals (:id-to-condition-node beta-graph))
                                       :when (isa? (:type (:condition query-condition-node))
                                                   :clara.rules.engine/query-result)
                                       :let [query-type (:type (:condition query-condition-node))

                                             query-name (if (= "clara.query.default" (namespace query-type))
                                                          (name query-type)
                                                          (str (namespace query-type) "/" (name query-type)))]]
                                   [query-name query-type]))

        ;; parent-id, query tuples for the new rule do add.
        parent-ids-to-queries (for [[id query-node] (:id-to-production-node beta-graph)
                                    :when (query-name-to-type (:name (:query query-node)))]
                                [(get-in beta-graph [:backward-edges id]) query-node])]

    (loop [beta-graph beta-graph
           [[parent-ids query-node] & rest-parent-ids-to-queries] parent-ids-to-queries]

           ;; No more queries to wire, so return.
           (if (nil? parent-ids)
             beta-graph

             (let [generated-node-id (create-id-fn)
                   query (:query query-node)
                   query-type (query-name-to-type (:name query))
                   assert-query-result-rhs `(clara.rules.engine/insert-query-result! ~query-type ~'?__token__)]

               ;; Generate a rule to fire the results of the query
               (add-node beta-graph
                         (seq parent-ids) ; add-node schema expects ids to be sequential
                         generated-node-id
                         {:node-type :production
                          :production (cond-> {:lhs (:lhs query)
                                               :rhs assert-query-result-rhs}

                                        (:props query) (assoc :props (:props query))
                                        (:env query) (assoc :env (:env query)))}))))))

(sc/defn to-beta-graph :- schema/BetaGraph

  "Produces a description of the beta network."
  [productions :- #{schema/Production}]

  (let [id-counter (atom 0)
        create-id-fn (fn [] (swap! id-counter inc))

        beta-graph (reduce (fn [beta-graph production]
                             (binding [*compile-ctx* {:production production}]
                               (add-production production beta-graph create-id-fn)))

                           empty-beta-graph
                           productions)

        with-queries (wire-queries beta-graph create-id-fn)]

    with-queries))

(sc/defn ^:private compile-node
  "Compiles a given node description into a node usable in the network with the
   given children."
  [beta-node :- (sc/either schema/ConditionNode schema/ProductionNode)
   id :- sc/Int
   is-root :- sc/Bool
   children :- [sc/Any]
   compile-expr ] ; Function to do the evaluation.

  (let [{:keys [condition production query join-bindings]} beta-node

        condition (if (symbol? condition)
                    (.loadClass (clojure.lang.RT/makeClassLoader) (name condition))
                    condition)]

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
           (binding [*compile-ctx* {:condition condition
                                    :join-filter-expressions (:join-filter-expressions beta-node)
                                    :env (:env beta-node)
                                    :msg "compiling expression join node"}]
             (compile-expr id
                           (compile-join-filter (:join-filter-expressions beta-node)
                                                (:join-filter-join-bindings beta-node)
                                                (:env beta-node))))
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
         (binding [*compile-ctx* {:condition condition
                                  :join-filter-expressions (:join-filter-expressions beta-node)
                                  :env (:env beta-node)
                                  :msg "compiling negation with join filter node"}]
           (compile-expr id
                         (compile-join-filter (:join-filter-expressions beta-node)
                                              (:join-filter-join-bindings beta-node)
                                              (:env beta-node))))
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
       (binding [*compile-ctx* {:condition condition
                                :env (:env beta-node)
                                :msg "compiling test node"}]
         (compile-expr id
                       (compile-test (:constraints condition))))
       children)

      :accumulator
      ;; We create an accumulator that accepts the environment for the beta node
      ;; into its context, hence the function with the given environment.
      (let [compiled-node (binding [*compile-ctx* {:condition condition
                                                   :accumulator (:accumulator beta-node)
                                                   :env (:env beta-node)
                                                   :msg "compiling accumulator"}]
                            (compile-expr id
                                          (compile-accum (:accumulator beta-node) (:env beta-node))))
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
           (binding [*compile-ctx* {:condition condition
                                    :join-filter-expressions (:join-filter-expressions beta-node)
                                    :env (:env beta-node)
                                    :msg "compiling accumulate with join filter node"}]
             (compile-expr id
                           (compile-join-filter (:join-filter-expressions beta-node)
                                                (:join-filter-join-bindings beta-node)
                                                (:env beta-node))))
           (:result-binding beta-node)
           children
           join-bindings)

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
           join-bindings)))

      :production
      (eng/->ProductionNode
       id
       production
       (binding [*file* (:file (meta (:rhs production)))
                 *compile-ctx* {:production production
                                :msg "compiling production node"}]
         (compile-expr id
                       (with-meta (compile-action (:bindings beta-node)
                                                  (:rhs production)
                                                  (:env production))
                         (meta (:rhs production))))))

      :query
      (eng/->QueryNode
       id
       query
       (:params query)))))

(sc/defn ^:private compile-beta-graph :- {sc/Int sc/Any}
  "Compile the beta description to the nodes used at runtime."
  [{:keys [id-to-production-node id-to-condition-node forward-edges backward-edges]} :- schema/BetaGraph]
  (let [;; A local, memoized function that ensures that the same expression of
        ;; a given node :id is only compiled into a single function.
        ;; This prevents redundant compilation and avoids having a Rete node
        ;; :id that has had its expressions compiled into different
        ;; compiled functions.
        compile-expr (memoize (fn [id expr] (try-eval expr)))

        ;; Sort the ids to compile based on dependencies.
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
                                                          [dependent-id (s/difference dependencies newly-satisfied-ids)]))]

                             (recur (s/difference pending-ids newly-satisfied-ids)
                                    updated-edges
                                    (concat sorted-nodes newly-satisfied-ids)))))]

    (reduce (fn [id-to-compiled-nodes id-to-compile]

              ;; Get the condition or production node to compile
              (let [node-to-compile (get id-to-condition-node
                                         id-to-compile
                                         (get id-to-production-node id-to-compile))

                    ;; Get the children.
                    children (->> (get forward-edges id-to-compile)
                                 (select-keys id-to-compiled-nodes)
                                 (vals))]

                ;; Sanity check for our logic...
                (assert (= (count children)
                           (count (get forward-edges id-to-compile)))
                        "Each child should be compiled.")

                (if (= ::root-condition node-to-compile)
                  id-to-compiled-nodes
                  (assoc id-to-compiled-nodes
                         id-to-compile
                         (compile-node node-to-compile
                                       id-to-compile
                                       ;; 0 is the id of the root node.
                                       (= #{0} (get backward-edges id-to-compile))
                                       children
                                       compile-expr))))
              )
            {}
            ids-to-compile)))


(sc/defn to-alpha-graph :- [schema/AlphaNode]
  "Returns a sequence of [condition-fn, [node-ids]] tuples to represent the alpha side of the network."
  [beta-graph :- schema/BetaGraph]

  ;; Create a sequence of tuples of conditions + env to beta node ids.
  (let [condition-to-node-ids (for [[id node] (:id-to-condition-node beta-graph)
                                    :when (:condition node)]
                                [[(:condition node) (:env node)] id])

        ;; Merge common conditions together.
        condition-to-node-map (reduce
                               (fn [node-map [[condition env] node-id]]

                                 ;; Can't use simple update-in because we need to ensure
                                 ;; the value is a vector, not a list.
                                 (if (get node-map [condition env])
                                   (update-in node-map [[condition env]] conj node-id)
                                   (assoc node-map [condition env] [node-id])))
                               {}
                               condition-to-node-ids)]

    ;; Compile conditions into functions.
    (vec
     (for [[[condition env] node-ids] condition-to-node-map
           :when (:type condition) ; Exclude test conditions.
           ]

       (cond-> {:condition condition
                :beta-children (distinct node-ids)}
         (seq env) (assoc :env env))))))


(sc/defn compile-alpha-nodes :- [{:type sc/Any
                                  :alpha-fn sc/Any ;; TODO: is a function...
                                  (sc/optional-key :env) {sc/Keyword sc/Any}
                                  :children [sc/Num]}]
  [alpha-nodes :- [schema/AlphaNode]]
  (for [{:keys [condition beta-children env]} alpha-nodes
        :let [{:keys [type constraints fact-binding args]} condition
              cmeta (meta condition)]]

    (cond-> {:type (effective-type type)
             :alpha-fn (binding [*file* (or (:file cmeta) *file*)
                                 *compile-ctx* {:condition condition
                                                :env env
                                                :msg "compiling alpha node"}]
                         (try-eval (with-meta (compile-condition
                                               type (first args)  constraints
                                               fact-binding env)
                                     (meta condition))))
             :children beta-children}
      env (assoc :env env))))

(sc/defn build-network
  "Constructs the network from compiled beta tree and condition functions."
  [id-to-node :- {sc/Int sc/Any}
   beta-roots
   alpha-fns
   productions]

  (let [beta-nodes (vals id-to-node)

        production-nodes (for [node beta-nodes
                               :when (= ProductionNode (type node))]
                           node)

        query-nodes (for [node beta-nodes
                          :when (= QueryNode (type node))]
                      node)

        query-map (into {} (for [query-node query-nodes

                                 ;; Queries can be looked up by reference or by name;
                                 entry [[(:query query-node) query-node]
                                        [(:name (:query query-node)) query-node]]]
                             entry))

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

    (strict-map->Rulebase
     {:alpha-roots alpha-map
      :beta-roots beta-roots
      :productions productions
      :production-nodes production-nodes
      :query-nodes query-map
      :id-to-node id-to-node})))

(defn- create-get-alphas-fn
  "Returns a function that given a sequence of facts,
  returns a map associating alpha nodes with the facts they accept."
  [fact-type-fn ancestors-fn merged-rules]

  ;; We preserve a map of fact types to alpha nodes for efficiency,
  ;; effectively memoizing this operation.
  (let [alpha-map (atom {})

        ;; If a customized fact-type-fn is provided,
        ;; we must use a specialized grouping function
        ;; that handles internal control types that may not
        ;; follow the provided type function.
        fact-grouping-fn (if (= fact-type-fn type)
                           type
                           (fn [fact]
                             (if (isa? (type fact) :clara.rules.engine/system-type)
                               ;; Internal system types always use Clojure's type mechanism.
                               (type fact)
                               ;; All other types defer to the provided function.
                               (fact-type-fn fact))))]
    (fn [facts]
      (for [[fact-type facts] (platform/tuned-group-by fact-grouping-fn facts)]

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
  [productions :- #{schema/Production}
   options :- {sc/Keyword sc/Any}]
  (let [beta-graph (to-beta-graph productions)
        beta-tree (compile-beta-graph beta-graph)
        beta-root-ids (-> beta-graph :forward-edges (get 0)) ; 0 is the id of the virtual root node.
        beta-roots (vals (select-keys beta-tree beta-root-ids))
        alpha-nodes (compile-alpha-nodes (to-alpha-graph beta-graph))
        rulebase (build-network beta-tree beta-roots alpha-nodes productions)
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
                   :memory (eng/local-memory rulebase transport activation-group-sort-fn activation-group-fn get-alphas-fn)
                   :transport transport
                   :listeners (get options :listeners  [])
                   :get-alphas-fn get-alphas-fn})))

(defn mk-session
  "Creates a new session using the given rule source. The resulting session
  is immutable, and can be used with insert, retract, fire-rules, and query functions."
  ([sources-and-options]
   (let [sources (take-while (complement keyword?) sources-and-options)
         options (apply hash-map (drop-while (complement keyword?) sources-and-options))
         productions (into #{}
                           (mapcat
                            #(if (satisfies? IRuleSource %)
                               (load-rules %)
                               %))
                           sources)] ; Load rules from the source, or just use the input as a seq.
     (if-let [session (get @session-cache [productions options])]
       session
       (let [session (mk-session* productions options)]

         ;; Cache the session unless instructed not to.
         (when (get options :cache true)
           (swap! session-cache assoc [productions options] session))

         ;; Return the session.
         session)))))
