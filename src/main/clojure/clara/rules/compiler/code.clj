(ns clara.rules.compiler.code
  (:require
    [clara.rules.engine.nodes :as nodes]
    [clara.rules.schema :as schema]
    [schema.core :as sc])
  (:import [clara.rules.engine.nodes AlphaNode ProductionNode QueryNode]))

;; Protocol for loading rules from some arbitrary source.
(defprotocol IRuleSource (load-rules [source]))

;; A rulebase -- essentially an immutable Rete network with a collection of alpha and beta nodes and supporting structure.
(sc/defrecord Rulebase [;; Map of matched type to the alpha nodes that handle them.
                        alpha-roots :- {sc/Any [AlphaNode]}
                        ;; Root beta nodes (join, accumulate, etc.)
                        beta-roots :- [nodes/BetaNode]
                        ;; Productions in the rulebase.
                        productions :- [schema/Production]
                        ;; Production nodes.
                        production-nodes :- [ProductionNode]
                        ;; Map of queries to the nodes hosting them.
                        query-nodes :- {sc/Any QueryNode}
                        ;; May of id to one of the beta nodes (join, accumulate, etc)
                        id-to-node :- {sc/Num nodes/BetaNode}])