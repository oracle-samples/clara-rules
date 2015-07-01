(ns clara.rules.compiler.trees
  (:require 
    [clojure.zip] [clojure.set :as s]
    [clara.rules.compiler.util :as util]
    [clara.rules.compiler.expressions :as expr]
    [clara.rules.schema :as schema] [schema.core :as sc]))

(defn add-to-beta-tree
  "Adds a sequence of conditions and the corresponding production to the beta tree."
  [beta-nodes
   [[condition env] & more]
   bindings
   production]
  (let [node-type (expr/condition-type condition)
        accumulator (:accumulator condition)
        result-binding (:result-binding condition) ; Get the optional result binding used by accumulators.
        condition (cond
                   (= :negation node-type) (second condition)
                   accumulator (:from condition)
                   :default condition)

        ;; Get the non-equality unifications so we can handle them
        join-filter-expressions (if (and (or (= :accumulator node-type)
                                             (= :negation node-type))
                                         (some expr/non-equality-unification? (:constraints condition)))

                                    (assoc condition :constraints (filterv expr/non-equality-unification? (:constraints condition)))

                                    nil)

        ;; Remove instances of non-equality constraints from accumulator
        ;; and negation nodes, since those are handled with specialized node implementations.
        condition (if (or (= :accumulator node-type)
                          (= :negation node-type))
                    (assoc condition
                      :constraints (into [] (remove expr/non-equality-unification? (:constraints condition)))
                      :original-constraints (:constraints condition))

                    condition)

        ;; For the sibling beta nodes, find a match for the candidate.
        matching-node (first (for [beta-node beta-nodes
                                   :when (and (= condition (:condition beta-node))
                                              (= node-type (:node-type beta-node))
                                              (= env (:env beta-node))
                                              (= result-binding (:result-binding beta-node))
                                              (= accumulator (:accumulator beta-node)))]
                               beta-node))

        other-nodes (remove #(= matching-node %) beta-nodes)
        cond-bindings (expr/variables-as-keywords (:constraints condition))

        ;; Create either the rule or query node, as appropriate.
        production-node (if (:rhs production)
                          {:node-type :production
                           :production production}
                          {:node-type :query
                           :query production})]

    (when (some #{result-binding} bindings)
      (throw (ex-info (str "Using accumulator result bindings in other expressions in a rule"
                           " left-hand side is currently not supported. Binding: "
                           result-binding
                           " in production " (or (:name production) production) )
                      {:condition condition})))

    (vec
     (conj
      other-nodes
      (if condition
        ;; There are more conditions, so recurse.
        (if matching-node
          (assoc matching-node
            :children
            (add-to-beta-tree (:children matching-node)
                              more
                              (cond-> (s/union bindings cond-bindings)
                                      result-binding (conj result-binding)
                                      (:fact-binding condition) (conj (:fact-binding condition)))
                              production))

          (cond->
           {:node-type node-type
            :condition condition
            :children (add-to-beta-tree []
                                        more
                                        (cond-> (s/union bindings cond-bindings)
                                                result-binding (conj result-binding)
                                                (:fact-binding condition) (conj (:fact-binding condition)))
                                        production)
            :env (or env {})}

           ;; Add the join bindings to join, accumulator or negation nodes.
           (#{:join :negation :accumulator} node-type) (assoc :join-bindings (s/intersection bindings cond-bindings))

           accumulator (assoc :accumulator accumulator)

           result-binding (assoc :result-binding result-binding)

           join-filter-expressions (assoc :join-filter-expressions join-filter-expressions)))

        ;; There are no more conditions, so add our query or rule.
        (if matching-node
          (update-in matching-node [:children] conj production-node)
          production-node))))))

(sc/defn to-beta-tree :- [schema/BetaNode]
  "Convert a sequence of rules and/or queries into a beta tree. Returns each root."
  [productions :- [schema/Production]]
  (let [conditions (mapcat expr/get-conds productions)

        raw-roots (reduce
                   (fn [beta-roots [conditions production]]
                    (add-to-beta-tree beta-roots conditions #{} production))
                   []
                   conditions)

        nodes (for [root raw-roots
                    node (tree-seq :children :children root)]
                node)

        ;; Sort nodes so the same id is assigned consistently,
        ;; then map the to corresponding ids.
        nodes-to-id (zipmap
                     (sort util/gen-compare nodes)
                     (range))

        ;; Anonymous function to walk the nodes and
        ;; assign identifiers to them.
        assign-ids-fn (fn assign-ids [node]
                        (if (:children node)
                          (merge node
                                 {:id (nodes-to-id node)
                                  :children (map assign-ids (:children node))})
                          (assoc node :id (nodes-to-id node))))]

    ;; Assign IDs to the roots and return them.
    (map assign-ids-fn raw-roots)))

(sc/defn to-alpha-tree :- [schema/AlphaNode]
  "Returns a sequence of [condition-fn, [node-ids]] tuples to represent the alpha side of the network."
  [beta-roots :- [schema/BetaNode]]

  ;; Create a sequence of tuples of conditions + env to beta node ids.
  (let [condition-to-node-ids (for [root beta-roots
                                    node (tree-seq :children :children root)
                                    :when (:condition node)]
                                [[(:condition node) (:env node)] (:id node)])

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
               env (assoc :env env))))))