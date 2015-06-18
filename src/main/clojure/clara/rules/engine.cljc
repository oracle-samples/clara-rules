(ns clara.rules.engine
  "The Clara rules engine. Most users should use only the clara.rules namespace."
  (:require
    [clara.rules.memory :as mem] [clara.rules.listener :as l]
    [clara.rules.engine.wme :as wme] [clara.rules.engine.state :as state]
    #?(:clj [clara.rules.engine.sessions :as sessions]
            :cljs [clara.rules.engine.sessions :as sessions :refer [LocalSession]])
    [clara.rules.engine.impl :as impl]
    [clara.rules.platform :as platform]
    [schema.core :as s] [clojure.string :as string])
  #?(:clj (:import [clara.rules.engine.sessions LocalSession])))


;; Token with no bindings, used as the root of beta nodes.
(def empty-token (wme/->Token [] {}))

;; Schema for the structure returned by the components
;; function on the session protocol.
;; This is simply a comment rather than first-class schema
;; for now since it's unused for validation and created
;; undesired warnings as described at https://groups.google.com/forum/#!topic/prismatic-plumbing/o65PfJ4CUkI
(comment

  (def session-components-schema
    {:rulebase s/Any
     :memory s/Any
     :transport s/Any
     :listeners [s/Any]
     :get-alphas-fn s/Any}))

;;
;; Hide implementation to outsiders

(defn insert [session facts]
  (impl/insert session facts))

(defn retract [session facts]
  (impl/retract session facts))

(defn fire-rules [session]
  (impl/fire-rules session))

(defn query [session query params]
  (impl/query session query params))

(defn alpha-activate [node facts memory transport listener]
  (impl/alpha-activate node facts memory transport listener))
  
(defn alpha-retract [node facts memory transport listener]
  (impl/alpha-retract node facts memory transport listener))

(defn right-activate-reduced [node join-bindings reduced  memory transport listener]
  (impl/right-activate-reduced node join-bindings reduced  memory transport listener))

(defn components [session]
  (impl/components session))

(defn insert-facts!
  "Perform the actual fact insertion, optionally making them unconditional."
  [facts unconditional]
  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} state/*current-session*
        {:keys [node token]} state/*rule-context*]

    ;; Update the insertion count.
    (swap! insertions + (count facts))

    ;; Track this insertion in our transient memory so logical retractions will remove it.
    (if unconditional
      (l/insert-facts! listener facts)
      (do
        (mem/add-insertions! transient-memory node token facts)
        (l/insert-facts-logical! listener node token facts)
        ))

    (swap! (:pending-updates state/*current-session*) into [(wme/->PendingUpdate :insert facts)])))

(defn conj-rulebases
  "DEPRECATED. Simply concat sequences of rules and queries.

   Conjoin two rulebases, returning a new one with the same rules."
  [base1 base2]
  (concat base1 base2))

(defn assemble
  "Assembles a session from the given components, which must be a map
   containing the following:

   :rulebase A recorec matching the clara.rules.compiler/Rulebase structure.
   :memory An implementation of the clara.rules.memory/IMemoryReader protocol
   :transport An implementation of the clara.rules.engine/ITransport protocol
   :listeners A vector of listeners implementing the clara.rules.listener/IPersistentListener protocol
   :get-alphas-fn The function used to return the alpha nodes for a fact of the given type."

  [{:keys [rulebase memory transport listeners get-alphas-fn]}]
  (LocalSession. rulebase
                 memory
                 transport
                 (if (> (count listeners) 0)
                   (l/delegating-listener listeners)
                   l/default-listener)
                 get-alphas-fn))

(defn local-memory
  "Returns a local, in-process working memory."
  [rulebase transport activation-group-sort-fn activation-group-fn]
  (let [memory (mem/to-transient (mem/local-memory rulebase activation-group-sort-fn activation-group-fn))]
    (doseq [beta-node (:beta-roots rulebase)]
      (impl/left-activate beta-node {} [empty-token] memory transport l/default-listener))
    (mem/to-persistent! memory)))
