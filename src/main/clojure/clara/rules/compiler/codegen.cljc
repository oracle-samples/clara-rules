(ns clara.rules.compiler.codegen
  (:require
    #?(:clj [clara.rules.engine.nodes :as nodes]
            :cljs [clara.rules.engine.nodes :as nodes :refer [AlphaNode ProductionNode QueryNode]])
    [clara.rules.schema :as schema]
    [schema.core :as sc])
  #?(:clj
      (:import [clara.rules.engine.nodes AlphaNode ProductionNode QueryNode])))

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


(sc/defn build-network
  "Constructs the network from compiled beta tree and condition functions."
  [beta-roots alpha-fns productions]

  (let [beta-nodes (for [root beta-roots
                         node (tree-seq :children :children root)]
                     node)

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

        ;; Map of node ids to beta nodes.
        id-to-node (into {} (for [node beta-nodes]
                                 [(:id node) node]))

        ;; type, alpha node tuples.
        alpha-nodes (for [{:keys [type alpha-fn children env]} alpha-fns
                          :let [beta-children (map id-to-node children)]]
                      [type (nodes/->AlphaNode env beta-children alpha-fn)])

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
