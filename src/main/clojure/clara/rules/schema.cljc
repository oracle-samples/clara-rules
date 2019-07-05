(ns clara.rules.schema
  "Schema definition of Clara data structures using Prismatic's Schema library. This includes structures for rules and queries, as well as the schema
   for the underlying Rete network itself. This can be used by tools or other libraries working with rules."
  (:require [schema.core :as s]))


(s/defn condition-type :- (s/enum :or :not :and :exists :fact :accumulator :test)
  "Returns the type of node in a LHS condition expression."
  [condition]
  (if (map? condition) ; Leaf nodes are maps, per the schema

    (cond
     (:type condition) :fact
     (:accumulator condition) :accumulator
     :else :test)

    ;; Otherwise the node must a sequential that starts with the boolean operator.
    (first condition)))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rule and query structure schema.

(def SExpr
  (s/pred seq? "s-expression"))

(def FactCondition
  {:type s/Any ;(s/either s/Keyword (s/pred symbol?))
   :constraints [SExpr]
   ;; Original constraints preserved for tooling in case a transformation was applied to the condition.
   (s/optional-key :original-constraints) [SExpr]
   (s/optional-key :fact-binding) s/Keyword
   (s/optional-key :args) s/Any
   })

(def AccumulatorCondition
  {:accumulator s/Any
   :from FactCondition
   (s/optional-key :result-binding) s/Keyword})

(def TestCondition
  {:constraints [SExpr]})

(def LeafCondition
  (s/conditional
   :type FactCondition
   :accumulator AccumulatorCondition
   :else TestCondition))

(declare Condition)

(def BooleanCondition
  [(s/one (s/enum :or :not :and :exists) "operator")
   (s/recursive #'Condition)])

(def Condition
  (s/conditional
   sequential? BooleanCondition
   map? LeafCondition))

(def Rule
  {;; :ns-name is currently used to eval the :rhs form of a rule in the same
   ;; context that it was originally defined in.  It is optional and only used
   ;; when given.  It may be used for other purposes in the future.
   (s/optional-key :ns-name) s/Symbol
   (s/optional-key :name) (s/cond-pre s/Str s/Keyword)
   (s/optional-key :doc) s/Str
   (s/optional-key :props) {s/Keyword s/Any}
   (s/optional-key :env) {s/Keyword s/Any}
   :lhs [Condition]
   :rhs s/Any})

(def Query
  {(s/optional-key :name) (s/cond-pre s/Str s/Keyword)
   (s/optional-key :doc) s/Str
   (s/optional-key :props) {s/Keyword s/Any}
   (s/optional-key :env) {s/Keyword s/Any}
   :lhs [Condition]
   :params #{s/Keyword}})

(def Production
  (s/conditional
   :rhs Rule
   :else Query))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema for the Rete network itself.

(def ConditionNode
  {:node-type (s/enum :join :negation :test :accumulator)
   :condition LeafCondition

   ;; Captured environment in which the condition was defined, like closed variables.
   ;; Most rules (such as those defined by defrule) have no surrounding
   ;; environment, but user generated rules might.
   (s/optional-key :env) {s/Keyword s/Any}

   ;; Variables used to join to other expressions in the network.
   (s/optional-key :join-bindings) #{s/Keyword}

   ;; Variable bindings used by expressions in this node.
   :used-bindings #{s/Keyword}

   ;; Variable bindings used in the constraints that are not present in the ancestors of this node.
   :new-bindings #{s/Keyword}

   ;; An expression used to filter joined data.
   (s/optional-key :join-filter-expressions) LeafCondition

   ;; Bindings used to perform non-hash joins in the join filter expression.
   ;; this is a subset of :used-bindings.
   (s/optional-key :join-filter-join-bindings) #{s/Keyword}

   ;; The expression to create the accumulator.
   (s/optional-key :accumulator) s/Any

   ;; The optional fact or accumulator result binding.
   (s/optional-key :result-binding) s/Keyword})

(def ProductionNode
  {:node-type (s/enum :production :query)

   ;; Rule for rule nodes.
   (s/optional-key :production) Rule

   ;; Query for query nodes.
   (s/optional-key :query) Query

   ;; Bindings used in the rule right-hand side.
   (s/optional-key :bindings) #{s/Keyword}})

;; Alpha network schema.
(def AlphaNode
  {:id s/Int
   :condition FactCondition
   ;; Opional environment for the alpha node.
   (s/optional-key :env) {s/Keyword s/Any}
   ;; IDs of the beta nodes that are the children.
   :beta-children [s/Num]})

;; A graph representing the beta side of the rete network.
(def BetaGraph
  {;; Edges from parent to child nodes.
   :forward-edges {s/Int #{s/Int}}

   ;; Edges from child to parent nodes.
   :backward-edges {s/Int #{s/Int}}

   ;; Map of identifier to condition nodes.
   :id-to-condition-node {s/Int (s/cond-pre (s/eq :clara.rules.compiler/root-condition)
                                          ConditionNode)}

   ;; Map of identifier to query or rule nodes.
   :id-to-production-node {s/Int ProductionNode}

   ;; Map of identifier to new bindings created by the corresponding node.
   :id-to-new-bindings {s/Int #{s/Keyword}}})

(defn tuple
  "Given `items`, a list of schemas, will generate a schema to validate that a vector contains and is in the order provided
   by `items`."
  [& items]
  (s/constrained [s/Any]
                 (fn [tuple-vals]
                   (and (= (count tuple-vals)
                           (count items))
                        (every? nil? (map s/check items tuple-vals))))
                 "tuple"))

(def NodeCompilationValue
  (s/constrained {s/Keyword s/Any}
                 (fn [compilation]
                   (let [expr-keys #{:alpha-expr :action-expr :join-filter-expr :test-expr :accum-expr}]
                     (some expr-keys (keys compilation))))
                 "node-compilation-value"))

(def NodeCompilationContext
  (s/constrained NodeCompilationValue
                 (fn [compilation]
                   (let [xor #(and (or %1 %2)
                                   (not (and %1 %2)))]
                     (and (contains? compilation :compile-ctx)
                          (contains? (:compile-ctx compilation) :msg)
                          (xor (contains? (:compile-ctx compilation) :condition)
                               (contains? (:compile-ctx compilation) :production)))))
                 "node-compilation-context"))

;; A map of [<node-id> <field-name>] to SExpression, used in compilation of the rulebase.
(def NodeExprLookup
  ;; schema should be NodeCompilationContext in standard compilation,
  ;; but during serde it might be either as :compile-ctx is only used for compilation failures
  ;; and can be disabled post compilation.
  {(tuple s/Int s/Keyword) (tuple SExpr (s/conditional :compile-ctx NodeCompilationContext
                                                       :else NodeCompilationValue))})

;; An evaluated version of the schema mentioned above.
(def NodeFnLookup
  ;; This schema uses a relaxed version of NodeCompilationContext as once the expressions
  ;; have been eval'd there is technically no need for compile-ctx to be maintained except for
  ;; deserialization. In such events the compile-ctx would only be valuable when the environment
  ;; where the Session is being deserialized doesn't match that of the serialization, ie functions
  ;; and symbols cannot be resolved on the deserialization side.
  {(tuple s/Int s/Keyword) (tuple (s/pred ifn? "ifn?") NodeCompilationValue)})