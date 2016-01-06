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
  {(s/optional-key :name) s/Str
   (s/optional-key :doc) s/Str
   (s/optional-key :props) {s/Keyword s/Any}
   (s/optional-key :env) {s/Keyword s/Any}
   :lhs [Condition]
   :rhs s/Any
   })

(def Query
  {(s/optional-key :name) s/Str
   (s/optional-key :doc) s/Str
   (s/optional-key :props) {s/Keyword s/Any}
   (s/optional-key :env) {s/Keyword s/Any}
   :lhs [Condition]
   :params #{s/Keyword}
   })

(def Production
  (s/conditional
   :rhs Rule
   :else Query))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Schema for the Rete network itself.

(declare BetaNode)

(def JoinNode
  {:node-type (s/enum :join)
   :id s/Num
   :condition LeafCondition
   :join-bindings #{s/Keyword}
   (s/optional-key :join-filter-expressions) LeafCondition
   (s/optional-key :env) {s/Keyword s/Any}
   :children  [(s/recursive #'BetaNode)]})

(def NegationNode
  {:node-type (s/enum :negation)
   :id s/Num
   :condition LeafCondition
   (s/optional-key :join-filter-expressions) LeafCondition
   :join-bindings #{s/Keyword}
   (s/optional-key :env) {s/Keyword s/Any}
   :children  [(s/recursive #'BetaNode)]})

(def TestNode
  {:node-type (s/enum :test)
   :id s/Num
   :condition TestCondition
   (s/optional-key :env) {s/Keyword s/Any}
   :children  [(s/recursive #'BetaNode)]})

(def AccumulatorNode
  {:node-type (s/eq :accumulator)
   :id s/Num
   :condition LeafCondition
   (s/optional-key :join-filter-expressions) LeafCondition
   :accumulator s/Any
   (s/optional-key :env) {s/Keyword s/Any}
   :join-bindings #{s/Keyword}
   (s/optional-key :result-binding) s/Keyword
   :children  [(s/recursive #'BetaNode)]})

(def ProductionNode
  {:node-type (s/eq :production)
   :id s/Num
   :production Production })

(def QueryNode
  {:node-type (s/eq :query)
   :id s/Num
   :query Query})

;; Beta network schema.
(def BetaNode

  (s/conditional

   #(= (:node-type %) :join)
   JoinNode

   #(= (:node-type %) :negation)
   NegationNode

   #(= (:node-type %) :test)
   TestNode

   #(= (:node-type %) :accumulator)
   AccumulatorNode

   #(= (:node-type %) :production)
   ProductionNode

   #(= (:node-type %) :query)
   QueryNode))

;; Alpha network schema.
(def AlphaNode
  {:condition FactCondition
   ;; Opional environment for the alpha node.
   (s/optional-key :env) {s/Keyword s/Any}
   ;; IDs of the beta nodes that are the children.
   :beta-children [s/Num]})
