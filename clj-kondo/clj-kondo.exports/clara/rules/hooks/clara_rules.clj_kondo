(ns hooks.clara-rules
  (:require [clj-kondo.hooks-api :as api]
            [clojure.string :as str]
            [clojure.set :as set]))

(defn node-value
  [node]
  (when node
    (api/sexpr node)))

(defn node-type
  [node]
  (when node
    (api/tag node)))

(defn- binding-node?
  "determine if a symbol is a clara-rules binding symbol in the form `?<name>`"
  [node]
  (let [node-name (node-value node)]
    (and (symbol? node-name)
         (str/starts-with? (str node-name) "?"))))

(defn- fact-result-node?
  [node]
  (some-> node meta ::fact-result))

(defn- special-binding-node?
  "determine if a symbol is a clara-rules special binding symbol in the form `?__<name>__`"
  [node]
  (let [node-name (node-value node)
        node-name-str (str node-name)]
    (and (symbol? node-name)
         (str/starts-with? node-name-str "?__")
         (str/ends-with? node-name-str "__"))))

(defn- extract-special-tokens
  [node-seq]
  (->> (reduce (fn [token-seq node]
                 (cond
                   (and (= :token (node-type node))
                        (special-binding-node? node)
                        (nil? (namespace (node-value node))))
                   (cons node token-seq)

                   (seq (:children node))
                   (concat token-seq (extract-special-tokens (:children node)))

                   :else token-seq)) [] node-seq)
       (set)
       (sort-by node-value)))

(defn extract-arg-tokens
  [node-seq]
  (->> (reduce (fn [token-seq node]
                 (cond
                   (and (= :token (node-type node))
                        (symbol? (node-value node))
                        (not (binding-node? node))
                        (nil? (namespace (node-value node))))
                   (cons node token-seq)

                   (seq (:children node))
                   (concat token-seq (extract-arg-tokens (:children node)))

                   :else token-seq)) [] node-seq)
       (set)
       (sort-by node-value)))

(defn analyze-constraints
  "sequentially analyzes constraint expressions of clara rules and queries
  defined via defrule or defquery by sequentially analyzing its children lhs
  expressions and bindings."
  [fact-node condition allow-bindings? prev-bindings input-token production-args]
  (let [[condition-args constraint-seq]
        (cond
          (= :vector (node-type (first condition)))
          [(first condition) (rest condition)]

          (symbol? (node-value fact-node))
          [(api/vector-node (vec (extract-arg-tokens condition))) condition]

          :else [(api/vector-node []) condition])
        cond-binding-set (set (map node-value (:children condition-args)))
        ;;; if `this` bindings are not explicit, then add them anyways
        [this-input-bindings
         this-output-bindings] (when-not (contains? cond-binding-set 'this)
                                 [[[(api/token-node 'this) input-token]]
                                  [[(api/token-node '_) (api/token-node 'this)]]])
        args-binding-set (set (map node-value (:children production-args)))
        prev-bindings-set (->> (mapcat (comp :children first) prev-bindings)
                               (filter binding-node?)
                               (map node-value)
                               (set))
        constraint-bindings
        (loop [[constraint-expr & more] constraint-seq
               bindings []
               bindings-set (set/union prev-bindings-set args-binding-set)]
          (if (nil? constraint-expr)
            bindings
            (let [constraint (:children constraint-expr)
                  binding-nodes (let [binding-tokens (seq (filter binding-node? (rest constraint)))
                                      match-bindings-set (set/intersection (set (map node-value binding-tokens)) bindings-set)]
                                  (when (and allow-bindings?
                                             (contains? #{'= '==} (node-value (first constraint)))
                                             (or (seq (filter (complement binding-node?) (rest constraint)))
                                                 (not-empty match-bindings-set)))
                                    binding-tokens))
                  next-bindings-set (-> (set (map node-value binding-nodes))
                                        (set/difference bindings-set))
                  binding-expr-nodes (seq (filter (comp next-bindings-set node-value) binding-nodes))
                  [next-bindings-set next-bindings]
                  (if binding-nodes
                    [next-bindings-set
                     (cond->> [[(api/vector-node
                                 (vec binding-nodes))
                                constraint-expr]]
                       binding-expr-nodes
                       (concat [[(api/vector-node
                                  (vec binding-expr-nodes))
                                 input-token]]))]
                    [#{}
                     [[(api/vector-node
                        [(api/token-node '_)])
                       constraint-expr]]])]
              (recur more
                     (concat bindings next-bindings)
                     (set/union bindings-set next-bindings-set)))))

        input-bindings (when-not (empty? (node-value condition-args))
                         [[condition-args input-token]])]
    (concat input-bindings this-input-bindings constraint-bindings this-output-bindings)))

(defn analyze-conditions
  "sequentially analyzes condition expressions of clara rules and queries
  defined via defrule and defquery by taking into account the optional
  result binding, optional args bindings and sequentially analyzing
  its children constraint expressions."
  [condition-seq allow-bindings? prev-bindings input-token production-args]
  (loop [[condition-expr & more] condition-seq
         bindings []]
    (if (nil? condition-expr)
      bindings
      (let [condition (:children condition-expr)
            [result-token fact-node & condition] (if (= '<- (-> condition second node-value))
                                                   (cons (api/vector-node
                                                          [(vary-meta
                                                            (first condition)
                                                            assoc ::fact-result true)]) (nnext condition))
                                                   (cons (api/vector-node
                                                          [(api/token-node '_)]) condition))
            condition-bindings (cond
                                 (nil? condition)
                                 []

                                 (contains? #{:not} (node-value fact-node))
                                 (analyze-conditions condition false (concat prev-bindings bindings) input-token production-args)

                                 (contains? #{:or :and :exists} (node-value fact-node))
                                 (analyze-conditions condition allow-bindings? (concat prev-bindings bindings) input-token production-args)

                                 (and (= :list (node-type fact-node))
                                      (= :from (-> condition first node-value)))
                                 (analyze-conditions (rest condition) allow-bindings? (concat prev-bindings bindings) input-token production-args)

                                 :else
                                 (analyze-constraints fact-node condition allow-bindings? (concat prev-bindings bindings) input-token production-args))
            condition-tokens (->> (mapcat first condition-bindings)
                                  (filter binding-node?))
            result-vector (api/vector-node (vec (list* fact-node condition-tokens)))
            result-bindings [[result-token result-vector]]
            output-bindings (concat condition-bindings result-bindings)
            condition-output (->> (mapcat (comp :children first) output-bindings)
                                  (filter binding-node?)
                                  (set)
                                  (sort-by node-value))
            output-node (api/vector-node
                         (if (empty? condition-output)
                           [(api/token-node '_)]
                           (vec condition-output)))
            output-result-node (api/vector-node
                                (if (empty? condition-output)
                                  [(api/token-node nil)]
                                  (vec condition-output)))
            next-bindings [output-node
                           (api/list-node
                            (list
                             (api/token-node 'let)
                             (api/vector-node
                              (vec (apply concat output-bindings)))
                             output-result-node))]]
        (recur more (concat bindings [next-bindings]))))))

(defn analyze-parse-query-macro
  "analyze clara-rules parse-query macro"
  [{:keys [:node]}]
  (let [input-token (api/token-node (gensym 'input))
        input-args (api/vector-node
                    [input-token])
        [args conditions-node] (rest (:children node))
        condition-seq (:children conditions-node)
        special-tokens (extract-special-tokens condition-seq)
        special-args (when (seq special-tokens)
                       (api/vector-node
                        (vec special-tokens)))
        transformed-args (for [arg (:children args)]
                           (let [v (node-value arg)]
                             (if (keyword? v)
                               (api/token-node (symbol v))
                               arg)))
        production-args (api/vector-node
                         (if (empty? transformed-args)
                           [(api/token-node '_)]
                           (vec transformed-args)))
        condition-bindings (analyze-conditions condition-seq true [] input-token production-args)
        production-bindings (apply concat
                                   (when special-args
                                     [special-args input-token
                                      (api/token-node '_) special-args])
                                   [production-args input-token]
                                   condition-bindings)
        production-output (->> (mapcat (comp :children first) condition-bindings)
                               (filter binding-node?)
                               (set)
                               (sort-by node-value))
        production-result (api/list-node
                           (list
                            (api/token-node 'let)
                            (api/vector-node
                             (vec production-bindings))
                            (api/vector-node
                             (vec production-output))))
        fn-node (api/list-node
                 (list
                  (api/token-node 'fn)
                  (api/token-node 'query-fn)
                  input-args
                  production-result))
        new-node (api/map-node
                  [(api/keyword-node :production) fn-node])]
    {:node new-node}))

(defn analyze-defquery-macro
  "analyze clara-rules defquery macro"
  [{:keys [:node]}]
  (let [[production-name & children] (rest (:children node))
        production-docs (when (= :token (node-type (first children)))
                          (first children))
        children (if production-docs (rest children) children)
        production-opts (when (= :map (node-type (first children)))
                          (first children))
        input-token (api/token-node (gensym 'input))
        input-args (api/vector-node
                    [input-token])
        [args & condition-seq] (if production-opts (rest children) children)
        special-tokens (extract-special-tokens condition-seq)
        special-args (when (seq special-tokens)
                       (api/vector-node
                        (vec special-tokens)))
        transformed-args (for [arg (:children args)]
                           (let [v (node-value arg)
                                 m (meta arg)]
                             (if (keyword? v)
                               (cond-> (api/token-node (symbol v))
                                 (not-empty m)
                                 (vary-meta merge m))
                               arg)))
        production-args (api/vector-node
                         (if (empty? transformed-args)
                           [(api/token-node '_)]
                           (vec transformed-args)))
        condition-bindings (analyze-conditions condition-seq true [] input-token production-args)
        production-bindings (apply concat
                                   (when special-args
                                     [special-args input-token
                                      (api/token-node '_) special-args])
                                   [production-args input-token]
                                   condition-bindings)
        production-output (->> (mapcat (comp :children first) condition-bindings)
                               (filter binding-node?)
                               (set)
                               (sort-by node-value))
        production-result (api/list-node
                           (list
                            (api/token-node 'let)
                            (api/vector-node
                             (vec production-bindings))
                            (api/vector-node
                             (vec production-output))))
        fn-node (api/list-node
                 (cond-> (list (api/token-node 'fn) production-name)
                   production-docs (concat [production-docs])
                   :always (concat [input-args])
                   production-opts (concat [production-opts])
                   :always (concat [production-result])))
        new-node (vary-meta
                  (api/list-node
                   (list
                    (api/token-node 'def) production-name
                    (api/map-node
                     [(api/keyword-node :production) fn-node])))
                  merge {:clj-kondo/ignore [:clojure-lsp/unused-public-var]})]
    {:node new-node}))

(defn analyze-parse-rule-macro
  "analyze clara-rules parse-rule macro"
  [{:keys [:node]}]
  (let [input-token (api/token-node (gensym 'input))
        input-args (api/vector-node
                    [input-token])
        empty-args (api/vector-node [])
        production-seq (rest (:children node))
        special-tokens (extract-special-tokens production-seq)
        special-args (when (seq special-tokens)
                       (api/vector-node
                        (vec special-tokens)))
        [conditions-node body-node] production-seq
        condition-seq (:children conditions-node)
        condition-bindings (analyze-conditions condition-seq true [] input-token empty-args)
        production-bindings (apply concat
                                   (when special-args
                                     [special-args input-token
                                      (api/token-node '_) special-args])
                                   [(api/token-node '_) input-token]
                                   condition-bindings)
        production-output (->> (mapcat (comp :children first) condition-bindings)
                               (filter binding-node?)
                               (remove fact-result-node?)
                               (set)
                               (sort-by node-value))
        production-result (api/list-node
                           (list
                            (api/token-node 'let)
                            (api/vector-node
                             (vec production-bindings))
                            (api/vector-node
                             (vec production-output))
                            body-node))
        fn-node (api/list-node
                 (list
                  (api/token-node 'fn)
                  (api/token-node 'rule-fn)
                  input-args
                  production-result))
        new-node (api/map-node
                  [(api/keyword-node :production) fn-node])]
    {:node new-node}))

(defn analyze-defrule-macro
  "analyze clara-rules defrule macro"
  [{:keys [:node]}]
  (let [[production-name & children] (rest (:children node))
        production-docs (when (= :token (node-type (first children)))
                          (first children))
        children (if production-docs (rest children) children)
        production-opts (when (= :map (node-type (first children)))
                          (first children))
        input-token (api/token-node (gensym 'input))
        input-args (api/vector-node
                    [input-token])
        empty-args (api/vector-node [])
        production-seq (if production-opts (rest children) children)
        special-tokens (extract-special-tokens production-seq)
        special-args (when (seq special-tokens)
                       (api/vector-node
                        (vec special-tokens)))
        [body-seq _ condition-seq] (->> (partition-by (comp #{'=>} node-value) production-seq)
                                        (reverse))
        condition-bindings (analyze-conditions condition-seq true [] input-token empty-args)
        production-bindings (apply concat
                                   (when special-args
                                     [special-args input-token
                                      (api/token-node '_) special-args])
                                   [(api/token-node '_) input-token]
                                   condition-bindings)
        production-output (->> (mapcat (comp :children first) condition-bindings)
                               (filter binding-node?)
                               (remove fact-result-node?)
                               (set)
                               (sort-by node-value))
        production-result (api/list-node
                           (list*
                            (api/token-node 'let)
                            (api/vector-node
                             (vec production-bindings))
                            (api/vector-node
                             (vec production-output))
                            body-seq))
        fn-node (api/list-node
                 (cond-> (list (api/token-node 'fn) production-name)
                   production-docs (concat [production-docs])
                   :always (concat [input-args])
                   production-opts (concat [production-opts])
                   :always (concat [production-result])))
        new-node (vary-meta
                  (api/list-node
                   (list
                    (api/token-node 'def) production-name
                    (api/map-node
                     [(api/keyword-node :production) fn-node])))
                  merge {:clj-kondo/ignore [:clojure-lsp/unused-public-var]})]
    {:node new-node}))

(defn analyze-defhierarchy-macro
  "analyze clara-rules defhierarchy macro"
  [{:keys [:node]}]
  (let [[var-name & children] (rest (:children node))
        new-node (vary-meta
                  (api/list-node
                   (list
                    (api/token-node 'def)
                    var-name
                    (api/vector-node
                     children)))
                  merge {:clj-kondo/ignore [:clojure-lsp/unused-public-var]})]
    {:node new-node}))

(defn analyze-def-rules-test-macro
  [{:keys [:node]}]
  (let [[test-name test-params & test-body] (rest (:children node))
        {:keys [rules
                queries
                sessions]} (->> (:children test-params)
                                (partition 2)
                                (map (juxt (comp api/sexpr first) last))
                                (into {}))
        rules-seq (for [[name-node rule-node] (partition 2 (:children rules))]
                    [name-node (analyze-parse-rule-macro
                                {:node (api/list-node
                                        (list*
                                         (api/token-node 'clara.rules.dsl/parse-rule)
                                         (:children rule-node)))})])
        query-seq (for [[name-node query-node] (partition 2 (:children queries))]
                    [name-node (analyze-parse-query-macro
                                {:node (api/list-node
                                        (list*
                                         (api/token-node 'clara.rules.dsl/parse-query)
                                         (:children query-node)))})])
        session-seq (for [[name-node productions-node options-node] (partition 3 (:children sessions))
                          :let [args-seq (concat (:children productions-node)
                                                 (:children options-node))]]
                      [name-node (api/list-node
                                  (list
                                   (api/token-node 'clara.rules.compiler/mk-session)
                                   (api/vector-node
                                    (vec args-seq))))])
        args-seq (->> (concat rules-seq query-seq session-seq)
                      (apply concat))
        wrap-body (api/list-node
                   (list*
                    (api/token-node 'let)
                    (api/vector-node
                     (vec args-seq))
                    test-body))
        new-node (api/list-node
                  (list
                   (api/token-node 'clojure.test/deftest)
                   test-name
                   wrap-body))]
    {:node new-node}))
