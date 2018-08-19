(ns clara.macros
  "Direct use of this namespace is deprecated. Users can now
   simply use the defrule, defquery, and defsession macros
   in the clara.rules namespace. Users can simply pull those macros
   in like any other, for instance:

   (:require [clara.rules :refer [insert fire-rules query insert!]
                          :refer-macros [defrule defsession defquery]])
"
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem]
            [clara.rules.compiler :as com]
            [clara.rules.dsl :as dsl]
            [cljs.analyzer :as ana]
            [cljs.env :as env]
            [schema.core :as sc]
            [clara.rules.schema :as schema]
            [clojure.set :as s]))

;;; Clear productions stored in cljs.env/*compiler* for current namespace.
;;; Only exists for its side-effect, hence returns nil.
(defmacro clear-ns-productions!
  []
  (swap! env/*compiler* assoc-in [::productions (com/cljs-ns)] {})
  nil)

;; Store production in cljs.env/*compiler* under ::productions seq?
(defn- add-production [name production]
  (swap! env/*compiler* assoc-in [::productions (com/cljs-ns) name] production))

(defn- get-productions-from-namespace
  "Returns a map of names to productions in the given namespace."
  [namespace]
  ;; TODO: remove need for ugly eval by changing our quoting strategy.
  (let [productions (get-in @env/*compiler* [::productions namespace])]
    (map eval (vals productions))))

(defn- get-productions
  "Return the productions from the source"
  [source]
  (cond
   (symbol? source) (get-productions-from-namespace source)
   (coll? source) (seq source)
   :else (throw (IllegalArgumentException. "Unknown source value type passed to defsession"))))

(defn defrule!
  [name production]
  (add-production name production)
  `(def ~name
     ~production))

(defmacro defrule
  [name & body]
  (defrule! name (dsl/build-rule name body)))

(defn defquery!
  [name query]
  (add-production name query)
  `(def ~name
     ~query))

(defmacro defquery
  [name & body]
  (defquery! name (dsl/build-query name body)))

(sc/defn gen-beta-network :- [sc/Any] ; Returns a sequence of compiled nodes.
  "Generates the beta network from the beta tree. "
  ([node-ids :- #{sc/Int}              ; Nodes to compile.
    {:keys [id-to-production-node id-to-condition-node id-to-new-bindings forward-edges] :as beta-graph} :- schema/BetaGraph
    parent-bindings :- #{sc/Keyword}]
     (vec
      (for [id node-ids
            :let [beta-node (or (get id-to-condition-node id)
                                (get id-to-production-node id))

                  {:keys [condition production query join-bindings]} beta-node

                  child-ids (get forward-edges id)

                  constraint-bindings (com/variables-as-keywords (:constraints condition))

                  ;; Get all bindings from the parent, condition, and returned fact.
                  all-bindings (cond-> (s/union parent-bindings constraint-bindings)
                                       ;; Optional fact binding from a condition.
                                       (:fact-binding condition) (conj (:fact-binding condition))
                                       ;; Optional accumulator result.
                                       (:result-binding beta-node) (conj (:result-binding beta-node)))

                  new-bindings (get id-to-new-bindings id)]]

        (case (:node-type beta-node)

          :join
          (if (:join-filter-expressions beta-node)
            `(eng/->ExpressionJoinNode
              ~id
              '~condition
              ~(com/compile-join-filter (:join-filter-expressions beta-node)
                                        (:join-filter-join-bindings beta-node)
                                        (:new-bindings beta-node)
                                        {})
              ~(gen-beta-network child-ids beta-graph all-bindings)
              ~join-bindings)
            `(eng/->HashJoinNode
              ~id
              '~condition
              ~(gen-beta-network child-ids beta-graph all-bindings)
              ~join-bindings))

          :negation
          (if (:join-filter-expressions beta-node)
            `(eng/->NegationWithJoinFilterNode
              ~id
              '~condition
              ~(com/compile-join-filter (:join-filter-expressions beta-node)
                                        (:join-filter-join-bindings beta-node)
                                        (:new-bindings beta-node)
                                        {})
              ~(gen-beta-network child-ids beta-graph all-bindings)
              ~join-bindings)
            `(eng/->NegationNode
              ~id
              '~condition
              ~(gen-beta-network child-ids beta-graph all-bindings)
              ~join-bindings))

          :test
          `(eng/->TestNode
            ~id
            ~(com/compile-test (:constraints condition))
            ~(gen-beta-network child-ids beta-graph all-bindings))

          :accumulator
          (if (:join-filter-expressions beta-node)
            `(eng/->AccumulateWithJoinFilterNode
              ~id
              {:accumulator '~(:accumulator beta-node)
               :from '~condition}
              ~(:accumulator beta-node)
              ~(com/compile-join-filter (:join-filter-expressions beta-node)
                                        (:join-filter-join-bindings beta-node)
                                        (:new-bindings beta-node)
                                        {})
              ~(:result-binding beta-node)
              ~(gen-beta-network child-ids beta-graph all-bindings)
              ~join-bindings
              ~new-bindings)

            `(eng/->AccumulateNode
              ~id
              {:accumulator '~(:accumulator beta-node)
               :from '~condition}
              ~(:accumulator beta-node)
              ~(:result-binding beta-node)
              ~(gen-beta-network child-ids beta-graph all-bindings)
              ~join-bindings
              ~new-bindings))

          :production
          `(eng/->ProductionNode
           ~id
           '~production
           ;; NOTE:  This is a workaround around allowing the compiler to eval this
           ;; form in an unknown ns.  It will suffer from var shadowing problems described
           ;; @ https://github.com/cerner/clara-rules/issues/178.
           ;; A better solution may be to enhance dsl/resolve-vars to deal with shadowing
           ;; correctly in cljs.  This may be easier to do there since the compiler's
           ;; analyzer may be more exposed than on the clj side.
           ~(let [resolve-vars #(@#'dsl/resolve-vars (:rhs production)
                                                     ;; It is unlikely that passing the bindings matters,
                                                     ;; but all:fact-binding from the LHS conditions were
                                                     ;; made available here in the past.  However, all
                                                     ;; bindings begin with "?" which typically means
                                                     ;; that a rule wouldn't, or at least shouldn't for
                                                     ;; clarity, start the names of other locals or vars
                                                     ;; with "?".
                                                     (mapv (comp symbol name) all-bindings))]
              (com/compile-action all-bindings
                                  ;; Using private function for now as a workaround.
                                  (if (:ns-name production)
                                    (if (com/compiling-cljs?)
                                      (binding [cljs.analyzer/*cljs-ns* (:ns-name production)]
                                        (resolve-vars))
                                      (binding [*ns* (the-ns (:ns-name production))]
                                        (resolve-vars)))
                                    (resolve-vars))
                                  (:env production))))

          :query
          `(eng/->QueryNode
           ~id
           '~query
           ~(:params query))
          (throw (ex-info (str "Unknown node type " (:node-type beta-node)) {:node beta-node})))))))

(sc/defn ^:always-validate compile-alpha-nodes
  [alpha-nodes :- [schema/AlphaNode]]
  (vec
   (for [{:keys [id condition beta-children env]} alpha-nodes
         :let [{:keys [type constraints fact-binding args]} condition]]

     {:id id
      :type (com/effective-type type)
      :alpha-fn (com/compile-condition type (first args) constraints fact-binding env)
      :children (vec beta-children)})))

(defn productions->session-assembly-form
  [productions options]
  ;;; Calling com/names-unique here rather than in sources-and-options->session-assembly-form
  ;;; as a ClojureScript DSL may call productions->session-assembly-form if that DSL
  ;;; has its own mechanism for grouping rules which is different than the clara DSL.
  (com/validate-names-unique productions)
  (let [id-counter (atom 0)
        create-id-fn (fn [] (swap! id-counter inc))

        beta-graph (com/to-beta-graph productions create-id-fn)
        ;; Compile the children of the logical root condition.
        beta-network (gen-beta-network (get-in beta-graph [:forward-edges 0]) beta-graph #{})

        alpha-graph (com/to-alpha-graph beta-graph create-id-fn)
        alpha-nodes (compile-alpha-nodes alpha-graph)]
    
    `(let [beta-network# ~beta-network
           alpha-nodes# ~alpha-nodes
           productions# '~productions
           options# ~options]
       (clara.rules/assemble-session beta-network# alpha-nodes# productions# options#))))

(defn sources-and-options->session-assembly-form
  [sources-and-options]
  (let [sources (take-while #(not (keyword? %)) sources-and-options)
        options (apply hash-map (drop-while #(not (keyword? %)) sources-and-options))
        ;; Eval to unquote ns symbols, and to eval exprs to look up
        ;; explicit rule sources
        sources (eval (vec sources))
        productions (vec (for [source sources
                               production (get-productions source)]
                           production))]
    (productions->session-assembly-form productions options)))

(defmacro defsession
  "Creates a session given a list of sources and keyword-style options, which are typically ClojureScript namespaces.

  Each source is eval'ed at compile time, in Clojure (not ClojureScript.)

  If the eval result is a symbol, it is presumed to be a ClojureScript
  namespace, and all rules and queries defined in that namespace will
  be found and used.

  If the eval result is a collection, it is presumed to be a
  collection of productions. Note that although the collection must
  exist in the compiling Clojure runtime (since the eval happens at
  macro-expansion time), any expressions in the rule or query
  definitions will be executed in ClojureScript.

  Typical usage would be like this, with a session defined as a var:

  (defsession my-session 'example.namespace)

  That var contains an immutable session that then can be used as a starting point to create sessions with
  caller-provided data. Since the session itself is immutable, it can be safely used from multiple threads
  and will not be modified by callers. So a user might grab it, insert facts, and otherwise
  use it as follows:

   (-> my-session
     (insert (->Temperature 23))
     (fire-rules))
  "
  [name & sources-and-options]
  `(def ~name ~(sources-and-options->session-assembly-form sources-and-options)))
