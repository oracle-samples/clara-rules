(ns clara.rules.engine
  "The Clara rules engine. Most users should use only the clara.rules namespace."
  (:require
    [clara.rules.memory :as mem] [clara.rules.listener :as l]
    [clara.rules.compiler.codegen :as codegen]
    [clara.rules.engine.wme :as wme] [clara.rules.engine.state :as state]
    [clara.rules.engine.sessions :as sessions]
    [clara.rules.engine.protocols :as impl]
    [schema.core :as s] [clojure.string :as string]))

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

;; Cache of sessions for fast reloading.
(def ^:private session-cache (atom {}))

(defn clear-session-cache!
  "Clears the cache of reusable Clara sessions, so any subsequent sessions
   will be re-compiled from the rule definitions. This is intended for use
   by tooling or specialized needs; most users can simply specify the :cache false
   option when creating sessions."
  []
  (reset! session-cache {}))

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
        (l/insert-facts-logical! listener node token facts)))

    (swap! (:pending-updates state/*current-session*) into [(wme/->PendingUpdate :insert facts)])))

(defn conj-rulebases
  "DEPRECATED. Simply concat sequences of rules and queries.

   Conjoin two rulebases, returning a new one with the same rules."
  [base1 base2]
  (concat base1 base2))

(defn mk-session
  "Creates a new session using the given rule source. Thew resulting session
   is immutable, and can be used with insert, retract, fire-rules, and query functions."
  ([sources-and-options]

     ;; If an equivalent session has been created, simply reuse it.
     ;; This essentially memoizes this function unless the caller disables caching.
     (if-let [session (get @session-cache [sources-and-options])]
       session

       ;; Separate sources and options, then load them.
       (let [sources (take-while (complement keyword?) sources-and-options)
             options (apply hash-map (drop-while (complement keyword?) sources-and-options))
             productions (mapcat
                          #(if (satisfies? codegen/IRuleSource %)
                             (codegen/load-rules %)
                             %)
                          sources) ; Load rules from the source, or just use the input as a seq.
             session (sessions/mk-session* productions options)]

         ;; Cache the session unless instructed not to.
         (when (get options :cache true)
           (swap! session-cache assoc [sources-and-options] session))

         ;; Return the session.
         session))))
