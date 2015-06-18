(ns clara.rules.compiler.expressions
  (:require
    [clara.rules.schema :as schema] [schema.core :as sc]
    [clojure.set :as s]))

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

(defn ^:private expr-type [expression]
  (if (map? expression)
    :condition
    (first expression)))

(defn ^:private cartesian-join [lists lst]
  (if (seq lists)
    (let [[h & t] lists]
      (mapcat
       (fn [l]
         (map #(conj % l) (cartesian-join t lst)))
       h))
    [lst]))

(defn ^:private is-variable?
  "Returns true if the given expression is a variable (a symbol prefixed by ?)"
  [expr]
  (and (symbol? expr)
       (.startsWith (name expr) "?")))

(defn is-equals?
  [cmp]
  (and (symbol? cmp)
       (let [cmp-str (name cmp)]
         (or (= cmp-str "=")
             (= cmp-str "==")))))

(defn ^:private extract-from-constraint
  "Process and extract a test expression from the constraint. Returns a pair of [processed-constraint, test-constraint],
   which can be expanded into correspoding join and test nodes.
  The test may be nil if no extraction is necessary."
  [[op arg & rest :as constraint]]

  (let [has-variable? (fn [expr]
                        (some is-variable?
                              (flatten-expression expr)))

        binding-map (atom {})

        ;; Walks a constraint and updates the map of expressions
        ;; that should be bound to symbols for use in the downstream
        ;; test expression.
        process-form (fn process [[op & rest :as form]]
                       (if (not (has-variable? form))
                         (swap! binding-map assoc form (gensym "?__gen__"))
                         (doseq [item rest]
                           (if (sequential? item)
                             (process item)
                             (when (and (not (is-variable? item))
                                        (not (coll? item))) ;; Handle collection literals.
                               (swap! binding-map assoc item (gensym "?__gen__")))))))]

    ;; If the constraint is a simple binding or an expression without variables,
    ;; there is no test expression to extract.
    (if (or (not (has-variable? constraint))
            (and (is-equals? op)
                 ;; Handle bindings where variable is first.
                 (or (and (is-variable? arg)
                          (not (has-variable? rest)))
                     ;; Handle bindings with later variables.
                     (and (every? is-variable? rest)
                          (not (has-variable? arg))))))

      [[constraint] []]

      ;; The constraint had some cross-condition comparison or nested binding
      ;; that must be extracted to a test
      (do

        (process-form constraint)

        (if (not-empty @binding-map)

          ;; Replace the condition with the bindings needed for the test.
          [(for [[form sym] @binding-map]
             (list '= sym form))

           ;; Create a test condition that is the same as the original, but
           ;; with nested expressions replaced by the binding map.
           [(clojure.walk/postwalk (fn [form]
                                      (if-let [sym (get @binding-map form)]
                                        sym
                                        form))
                                    constraint)]]

          ;; No test bindings found, so simply keep the constraint as is.
          [[constraint] []])))))

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

        ;; DeMorgan's law converting conjunction to negated disjuctions.
        :and (to-dnf (into [:or] (for [grandchild (rest child)] [:not grandchild])))

        ;; DeMorgan's law converting disjuction to negated conjuctions.
        :or  (to-dnf (into [:and] (for [grandchild (rest child)] [:not grandchild])))))

    ;; For all others, recursively process the children.
    (let [children (map to-dnf (rest expression))
          ;; Get all conjunctions, which will not conain any disjunctions since they were processed above.
          conjunctions (filter #(#{:and :condition :not} (expr-type %)) children)]

      ;; If there is only one child, the and or or operator can simply be eliminated.
      (if (= 1 (count children))
        (first children)

        (condp = (expr-type expression)

          :and
          (let [disjunctions (map rest (filter #(= :or (expr-type %)) children))]
            (if (empty? disjunctions)
              (into [:and] (apply concat
                                  (for [child children]
                                    (if (= :and (expr-type child))
                                      (rest child)
                                      [child]))))
              (into [:or]
                    (for [c (cartesian-join disjunctions conjunctions)]
                      (into [:and] c)))))
          :or
          ;; Merge all child disjunctions into a single list.
          (let [disjunctions (mapcat rest (filter #(#{:or} (expr-type %)) children))]
            (into [:or] (concat disjunctions conjunctions))))))))

(defn condition-type
  "Returns the type of a single condition that has been transformed
   to disjunctive normal form. The types are: :negation, :accumulator, :test, and :join"
  [condition]
  (let [is-negation (= :not (first condition))
        accumulator (:accumulator condition)
        result-binding (:result-binding condition) ; Get the optional result binding used by accumulators.
        condition (cond
                   is-negation (second condition)
                   accumulator (:from condition)
                   :default condition)
        node-type (cond
                   is-negation :negation
                   accumulator :accumulator
                   (:type condition) :join
                   :else :test)]

    node-type))

(defn extract-tests
  "Pre-process the sequence of conditions, and returns a sequence of conditions that has
   constraints that must be expanded into tests properly expanded.
   For example, consider the following conditions
  [Temperature (= ?t1 temperature)]
  [Temperature (< ?t1 temperature)]
  The second temperature is doing a comparison, so we can't use our hash-based index to
  unify these items. Therefore we extract the logic into a separate test, so the above
  conditions are transformed into this:
  [Temperature (= ?t1 temperature)]
  [Temperature (= ?__gen__1234 temperature)]
  [:test (< ?t2 ?__gen__1234)]
  The comparison is transformed into binding to a generate bind variable,
  which is then available in the test node for comparison with other bindings."
  [conditions]
  ;; Look at the constraints under each condition. If the constraint does a comparison with an
  ;; item in an ancestor, do two things: modify the constraint to bind its internal items to a new
  ;; generated symbol, and add a test that does the expected check.
  (for [{:keys [type constraints] :as condition} conditions
        :let [extracted (map extract-from-constraint constraints)
              processed-constraints (mapcat first extracted)
              test-constraints (mapcat second extracted)]

        ;; Don't extract test conditions from tests themselves,
        ;; or items with no matching test constraints.
        expanded (if (or (= :test (condition-type condition))
                         (empty? test-constraints))
                   [condition]
                   ;; There were test constraints created, so the processed constraints
                   ;; and generated test condition.
                   [(assoc condition :constraints processed-constraints
                           :original-constraints constraints)
                    {:constraints test-constraints}  ])]

    expanded))

(defn ^:private condition-comp
  "Helper function to sort conditions to ensure bindings
   are created in the needed order. The current implementation
   simply pushes tests to the end (since they don't create new bindings)
   with accumulators before them, as they may rely on previously bound items
   to complete successfully."
  [cond1 cond2]

  (letfn [(cond-type [condition]
            (cond
             (:type condition) :condition
             (:accumulator condition) :accumulator
             (= :not (first condition)) :negation
             :default :test))]

    (case (cond-type cond1)
      ;; Conditions are always sorted ahead of non-conditions.
      :condition (not= :condition (cond-type cond2))

      ;; Negated conditions occur before tests and accumulators.
      :negation (boolean (#{:test :accumulator} (cond-type cond2)))

      ;; Accumulators are sorted before tests.
      :accumulator (= :test (cond-type cond2))

      ;; Tests are last.
      :test false)))

(defn ^:private check-unbound-variables
  "Checks the expanded condition to see if there are any potentially unbound
   variables being referenced."
  [expanded-conditions]
  (loop [ancestor-variables #{}
         [condition & rest] expanded-conditions]

    (let [variables (set (filter is-variable?
                                 (flatten-expression
                                  (if (= :accumulator (condition-type condition))
                                    (get-in condition [:from :constraints])
                                    (:constraints condition)))))]

      ;; Check only test expressions, since they don't bind variables.
      (when (and (= :test (condition-type condition))
                 (not (s/subset? variables ancestor-variables)))
        (throw (ex-info (str "Using variable that is not previously bound. This can happen "
                             "when a test expression uses a previously unbound variable, "
                             "or if a variable is referenced in a nested part of a parent "
                             "expression, such as (or (= ?my-expression my-field) ...). "
                             "Unbound variables: "
                             (s/difference variables ancestor-variables ))
                        {:variables (s/difference variables ancestor-variables )}) ))

      (when (seq? rest)

        ;; Recur with bound variables and fact and accumulator result bindings visible to children.
        (recur (cond-> (into ancestor-variables variables)
                       (:fact-binding condition) (conj (symbol (name (:fact-binding condition))))
                       (:result-binding condition) (conj (symbol (name (:result-binding condition)))))

               rest)))))

(defn get-conds
  "Returns a sequence of [condition environment] tuples and their corresponding productions."
  [production]

  (let [lhs-expression (into [:and] (:lhs production)) ; Add implied and.
        expression  (to-dnf lhs-expression)
        disjunctions (if (= :or (first expression))
                       (rest expression)
                       [expression])]

    ;; Now we've split the production into one ore more disjunctions that
    ;; can be processed independently. Commonality between disjunctions willl
    ;; be merged when building the Rete network.
    (for [disjunction disjunctions

          :let [conditions (if (and (vector? disjunction)
                                    (= :and (first disjunction)))
                             (rest disjunction)
                             [disjunction])

                conditions (extract-tests conditions)

                ;; Sort conditions, see the condition-comp function for the reason.
                sorted-conditions (sort condition-comp conditions)

                ;; Ensure there are no potentially unbound variables in use.
                _ (check-unbound-variables sorted-conditions)

                ;; Attach the conditions environment. TODO: narrow environment to those used?
                conditions-with-env (for [condition sorted-conditions]
                                      [condition (:env production)])]]

      [conditions-with-env production])))

(defn non-equality-unification? [expression]
  "Returns true if the given expression does a non-equality unification against a variable,
   indicating it can't be solved by simple unification."
  (let [found-complex (atom false)
        qualify-when-sym #(when-let [resolved (and (symbol? %)
                                                   (resolve %))]
                            (and (var? resolved)
                                 (symbol (-> resolved meta :ns ns-name name)
                                         (-> resolved meta :name name))))
        process-form (fn [form]
                       (when (and (list? form)
                                  (not (#{'clojure.core/= 'clojure.core/==}
                                        (qualify-when-sym (first form))))
                                  (some (fn [sym] (and (symbol? sym)
                                                      (.startsWith (name sym) "?")))
                                        (flatten-expression form)))

                         (reset! found-complex true))

                       form)]

    ;; Walk the expression to find use of a symbol that can't be solved by equality-based unificaiton.
    (doall (clojure.walk/postwalk process-form expression))

    @found-complex))