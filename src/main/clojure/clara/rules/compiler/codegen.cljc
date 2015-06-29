(ns clara.rules.compiler.codegen
  "Code generation and source code support fns"
  (:require
    [clara.rules.platform :as platform] [clara.rules.listener :as l] [clara.rules.schema :as schema]
    #?@(:clj [[clara.rules.compiler.helpers :as hlp] [clara.rules.engine.nodes :as nodes] [schema.core :as sc]])
    #?@(:cljs [[clara.rules.engine.nodes :as nodes :refer [AlphaNode ProductionNode QueryNode]] [schema.core :as sc :include-macros true]]))
  #?(:clj
      (:import [clara.rules.engine.nodes AlphaNode ProductionNode QueryNode])))

;; Protocol for loading rules from some arbitrary source.
(defprotocol IRuleSource (load-rules [source]))

;; Treate a symbol as a rule source, loading all items in its namespace.
#?(:clj
    (extend-type clojure.lang.Symbol
      IRuleSource
      (load-rules [sym]
        
        ;; Find the rules and queries in the namespace, shred them,
        ;; and compile them into a rule base.
        (->> (ns-interns sym)
          (vals) ; Get the references in the namespace.
          (filter #(or (:rule (meta %)) (:query (meta %)))) ; Filter down to rules and queries.
          (map deref)))))  ; Get the rules from the symbols.

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
#?(:clj
    (defn add-production [env name production]
      "Add a production to the given environment, normally cljs.env/*compiler* under ::productions seq?.
       Beware, this fn and get-productions need to be
       in the same name space (qualified ::production keyword)"
      (swap! env assoc-in [::productions (hlp/cljs-ns) name] production)))

#?(:clj
    (defn- get-productions-from-namespace 
      "Returns a map of names to productions in the given CLJS namespace."
      [namespace env]
      ;; TODO: remove need for ugly eval by changing our quoting strategy.
      (let [productions (get-in env [::productions namespace])]
        (map eval (vals productions)))))
  
#?(:clj
    (defn- get-cljs-productions
      "Return the productions from the source in CLJS space.
        We need the CLJS compiler environment."
      [source env]
      (cond
        (symbol? source) (get-productions-from-namespace source env)
        (coll? source) (seq source)
        :else (throw (IllegalArgumentException. "Unknown source value type passed to defsession")))))

#?(:clj
    (defn get-productions 
      "Returns a sequence of productions from the given runtime.
       Not doable directly in ClojureScript, only from Clojure macros.
       While compiling CLJS, productions need to be added the compiler environment.
       Beware, this fn and add-production need to be
       in the same name space (qualified ::production keyword)."
      ([sources runtime]
        (get-productions sources runtime {}))
      ([sources runtime env]
        (case runtime
          :clj
          (mapcat
            #(if (satisfies? IRuleSource %)
               (load-rules %)
               %) sources)
          :cljs
          (vec (for [source sources
                     production (get-cljs-productions source env)]
                 (do 
                 production)))
          (throw #?(:clj (Exception. (format "No such runtime %s") runtime)
                         :cljs (js/Error. (str "No such runtime " runtime))))))))

(defn create-get-alphas-fn
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
          (let [parents (conj (ancestors-fn fact-type) fact-type)
                alpha-roots (get merged-rules :alpha-roots)
                ;; Get all alpha nodes for all parents.
                new-nodes (distinct
                           (reduce
                            (fn [coll parent]
                              (concat coll (get alpha-roots parent)))
                            []
                            parents))]
            (swap! alpha-map assoc fact-type new-nodes)
            [new-nodes facts]))))))

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
      {:alpha-roots alpha-map :beta-roots beta-roots
       :productions productions :production-nodes production-nodes
       :query-nodes query-map :id-to-node id-to-node})))

