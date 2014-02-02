(ns clara.macros
  "Forward-chaining rules for Clojure. The primary API is in this namespace"
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem]
            [clara.rules.compiler :as com]
            [clara.rules.dsl :as dsl]            
            [cljs.analyzer :as ana]
            [cljs.env :as env]
            [clojure.set :as s])
  (:import [clara.rules.engine LocalTransport LocalSession]))


;; Store production in cljs.env/*compiler* under ::productions seq?
(defn- add-production [name production]
  (swap! env/*compiler* assoc-in [::productions (com/cljs-ns) name] production))

(defn- get-productions 
  "Returns a map of names to productions in the given namespace."
  [namespace]
  (get-in @env/*compiler* [::productions namespace]))

(defmacro defrule 
  [name & body]
  (let [doc (if (string? (first body)) (first body) nil)
        body (if doc (rest body) body)
        properties (if (map? (first body)) (first body) nil)
        definition (if properties (rest body) body)
        {:keys [lhs rhs]} (dsl/parse-rule-body definition)

        production (cond-> (dsl/parse-rule lhs rhs properties {})
                           name (assoc :name (clojure.core/name name))
                           doc (assoc :doc doc))]
    (add-production name production)
    `(def ~name
       ~production)))

(defmacro defquery 
  [name & body]
  (let [doc (if (string? (first body)) (first body) nil)
        binding (if doc (second body) (first body))
        definition (if doc (drop 2 body) (rest body) )

        query (cond-> (dsl/parse-query binding (dsl/parse-query-body definition) {})
                      name (assoc :name (clojure.core/name name))
                      doc (assoc :doc doc))]
    (add-production name query)
    `(def ~name
       ~query)))

(defn- gen-beta-network
  "Generates the beta network from the beta tree. "
  ([beta-nodes
    parent-bindings]
     (vec
      (for [beta-node beta-nodes
            :let [{:keys [condition children id production query join-bindings]} beta-node

                  constraint-bindings (com/variables-as-keywords (:constraints condition))

                  abc (spit "debug.txt" (str constraint-bindings parent-bindings condition))
                  ;; Get all bindings from the parent, condition, and returned fact.
                  all-bindings (cond-> (s/union parent-bindings constraint-bindings)
                                       (:fact-binding condition) (conj (:fact-binding condition)))]]

        (case (:node-type beta-node)

          :join
          `(eng/->JoinNode
            ~id
            '~condition
            ~(gen-beta-network children all-bindings)
            ~join-bindings)

          :negation
          `(eng/->NegationNode
            ~id
            '~condition
            ~(gen-beta-network children all-bindings)
            ~join-bindings)
          
          :test
          `(eng/->TestNode
            ~id
            ~(com/compile-test (:constraints condition))
            ~(gen-beta-network children all-bindings))

          :accumulator
          `(eng/->AccumulateNode
            ~id
            ~(:accumulator beta-node)
            ~(:result-binding beta-node)
            ~(gen-beta-network children all-bindings)
            ~join-bindings)

          :production
          `(eng/->ProductionNode
           ~id
           '~production
           ~(com/compile-action all-bindings (:rhs production) (:env production)))

          :query
          `(eng/->QueryNode
           ~id
           '~query
           ~(:params query))
          )))))

(defn- compile-alpha-nodes
  [alpha-nodes]
  (vec
   (for [{:keys [condition beta-children env]} alpha-nodes
         :let [{:keys [type constraints fact-binding args]} condition]]

     {:type (com/effective-type type)
      :alpha-fn (com/compile-condition type (first args) constraints fact-binding env)
      :children (vec beta-children)
      })))

(defmacro defsession 
  "Creates a sesson given a list of sources, which are typically ClojureScript namespaces."
  [name & sources]

  (let [;; Sources are typically quoted for consistency for the caller, so eval to unquote.
        sources (eval (vec sources))

        ;; TODO: remove need for ugly eval by changing our quoting strategy.
        productions (eval (vec (for [source sources 
                                     [name production] (get-productions source)]
                                 production)))

        beta-tree (com/to-beta-tree productions) 
        beta-network (gen-beta-network beta-tree #{})

        alpha-tree (com/to-alpha-tree beta-tree)
        alpha-nodes (compile-alpha-nodes alpha-tree)]

    `(let [beta-network# ~beta-network
           alpha-nodes# ~alpha-nodes
           productions# '~productions]
       
       (def ~name (clara.rules/assemble-session beta-network# alpha-nodes# productions# {})))))