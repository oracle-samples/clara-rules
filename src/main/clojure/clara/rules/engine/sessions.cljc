(ns clara.rules.engine.sessions
  "The purpose of this name space is to support session creation"
  (:require
    [clara.rules.memory :as mem]
     #?(:clj [clara.rules.compiler.codegen :as codegen] 
             :cljs [clara.rules.compiler.codegen :as codegen :refer [Rulebase]])
    [clara.rules.engine.protocols :as impl] [clara.rules.engine.wme :as wme]
    [clara.rules.listener :as l]
    [clara.rules.schema :as schema] [clara.rules.engine.transports :as transport]
    #?(:clj [clara.rules.engine.sessions.local :as local]
            :cljs [clara.rules.engine.sessions.local :as local :refer [LocalSession]])
    #?(:clj [schema.core :as sc] :cljs [schema.core :as sc :include-macros true]))
  #?(:clj (:import [clara.rules.engine.sessions.local LocalSession]
                   [clara.rules.compiler.codegen Rulebase])))


(defn assemble
  "Assembles a session from the given components, which must be a map
   containing the following:

   :rulebase A record matching the clara.rules.compiler.codegen/Rulebase structure.
   :memory An implementation of the clara.rules.memory/IMemoryReader protocol
   :transport An implementation of the clara.rules.engine/ITransport protocol
   :listeners A vector of listeners implementing the clara.rules.listener/IPersistentListener protocol
   :get-alphas-fn The function used to return the alpha nodes for a fact of the given type."

  [{:keys [rulebase memory transport listeners get-alphas-fn]}]
  (LocalSession. rulebase memory transport
                 (if (> (count listeners) 0)
                   (l/delegating-listener listeners)
                   l/default-listener)
                 get-alphas-fn))

(defn- local-memory
  "Returns a local, in-process working memory."
  [rulebase transport activation-group-sort-fn activation-group-fn]
  (let [memory (mem/to-transient (mem/local-memory rulebase activation-group-sort-fn activation-group-fn))]
    (doseq [beta-node (:beta-roots rulebase)]
      (impl/left-activate beta-node {} [wme/empty-token] memory transport l/default-listener))
    (mem/to-persistent! memory)))

(sc/defn mk-session*
  "Creates a session from a rulebase."
  [rulebase :- Rulebase
   options :- {sc/Keyword sc/Any}]
  (let [transport (transport/->transport :local)
        ;; The fact-type uses Clojure's type function unless overridden.
        fact-type-fn (get options :fact-type-fn type)
        
        ;; The ancestors for a logical type uses Clojure's ancestors function unless overridden.
        ancestors-fn (get options :ancestors-fn ancestors)
        
        ;; Default sort by higher to lower salience.
        activation-group-sort-fn (get options :activation-group-sort-fn >)
        
        ;; Activation groups use salience, with zero
        ;; as the default value.
        activation-group-fn (get options
                                 :activation-group-fn
                                 (fn [production]
                                   (or (some-> production :props :salience)
                                       0)))
        
        ;; Create a function that groups a sequence of facts by the collection
        ;; of alpha nodes they target.
        ;; We cache an alpha-map for facts of a given type to avoid computing
        ;; them for every fact entered.
        get-alphas-fn (codegen/create-get-alphas-fn fact-type-fn ancestors-fn rulebase)]
    (assemble {:rulebase rulebase
               :memory (local-memory rulebase transport activation-group-sort-fn activation-group-fn)
               :transport transport
               :listeners (get options :listeners  [])
               :get-alphas-fn get-alphas-fn})))


