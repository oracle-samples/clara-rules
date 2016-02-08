(ns clara.rules.schema
  "Schema definition of Clara data structures using Prismatic's Schema library. This includes structures for rules and queries, as well as the schema
   for the underlying Rete network itself. This can be used by tools or other libraries working with rules."
  (:require [schema.core :as s]
            [schema.macros :as sm]))


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
   (s/optional-key :args) s/Any})

;; Condition for calling queries from the left-hand side.
(def QueryCondition
  {:query s/Any ; The query to be called.
   :params {s/Keyword s/Any} ; Params to pass to the query.
   :constraints [SExpr] ; Constraints for the query.
   (s/optional-key :args) s/Any
   (s/optional-key :fact-binding) s/Keyword})

(def AccumulatorCondition
  {:accumulator s/Any
   :from (s/conditional
          :type FactCondition
          :query QueryCondition)
   (s/optional-key :result-binding) s/Keyword})

(def TestCondition
  {:constraints [SExpr]})

(def LeafCondition
  (s/conditional
   :type FactCondition
   :accumulator AccumulatorCondition
   :query QueryCondition
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
  {(s/optional-key :name) s/Str
   (s/optional-key :doc) s/Str
   (s/optional-key :props) {s/Keyword s/Any}
   (s/optional-key :env) {s/Keyword s/Any}
   :lhs [Condition]
   :rhs s/Any})

(def Query
  {(s/optional-key :name) s/Str
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
  {:condition FactCondition
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
   :id-to-condition-node {s/Int (s/either (s/eq :clara.rules.compiler/root-condition)
                                          ConditionNode)}

   ;; Map of identifier to query or rule nodes.
   :id-to-production-node {s/Int ProductionNode}})
