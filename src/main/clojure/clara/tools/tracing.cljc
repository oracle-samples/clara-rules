(ns clara.tools.tracing
  "Support for tracing state changes in a Clara session."
  (:require [clara.rules.listener :as l]
            [clara.rules.engine :as eng]))

(declare to-tracing-listener)

(deftype PersistentTracingListener [trace]
  l/IPersistentEventListener
  (to-transient [listener]
    (to-tracing-listener listener)))

(declare append-trace)

(deftype TracingListener [trace]
  l/ITransientEventListener
  (left-activate! [listener node tokens]
    (append-trace listener {:type :left-activate :node-id (:id node) :tokens tokens}))

  (left-retract! [listener node tokens]
    (append-trace listener {:type :left-retract :node-id (:id node) :tokens tokens}))

  (right-activate! [listener node elements]
    (append-trace listener {:type :right-activate :node-id (:id node) :elements elements}))

  (right-retract! [listener node elements]
    (append-trace listener {:type :right-retract :node-id (:id node) :elements elements}))

  (insert-facts! [listener node token facts]
    (append-trace listener {:type :add-facts :node node :token token :facts facts}))
  
  (alpha-activate! [listener node facts]
    (append-trace listener {:type :alpha-activate :facts facts}))

  (insert-facts-logical! [listener node token facts]
    (append-trace listener {:type :add-facts-logical :node node :token token :facts facts}))

  (retract-facts! [listener node token facts]
    (append-trace listener {:type :retract-facts :node node :token token :facts facts}))
  
  (alpha-retract! [listener node facts]
    (append-trace listener {:type :alpha-retract :facts facts}))

  (retract-facts-logical! [listener node token facts]
    (append-trace listener {:type :retract-facts-logical :node node :token token :facts facts}))

  (add-accum-reduced! [listener node join-bindings result fact-bindings]
    (append-trace listener {:type :accum-reduced
                            :node-id (:id node)
                            :join-bindings join-bindings
                            :result result
                            :fact-bindings fact-bindings}))

  (remove-accum-reduced! [listener node join-bindings fact-bindings]
    (append-trace listener {:type :remove-accum-reduced
                            :node-id (:id node)
                            :join-bindings join-bindings
                            :fact-bindings fact-bindings}))

  (add-activations! [listener node activations]
    (append-trace listener {:type :add-activations :node-id (:id node) :tokens (map :token activations)}))

  (remove-activations! [listener node activations]
    (append-trace listener {:type :remove-activations :node-id (:id node) :activations activations}))

  (fire-activation! [listener activation resulting-operations]
    (append-trace listener {:type :fire-activation :activation activation :resulting-operations resulting-operations}))

  (fire-rules! [listener node]
    (append-trace listener {:type :fire-rules :node-id (:id node)}))

  (activation-group-transition! [listener previous-group new-group]
    (append-trace listener {:type :activation-group-transition :new-group new-group :previous-group previous-group}))

  (to-persistent! [listener]
    (PersistentTracingListener. @trace)))

(defn- to-tracing-listener [^PersistentTracingListener listener]
  (TracingListener. (atom (.-trace listener))))

(defn- append-trace
  "Appends a trace event and returns a new listener with it."
  [^TracingListener listener event]
  (reset! (.-trace listener) (conj @(.-trace listener) event)))

(defn tracing-listener
  "Creates a persistent tracing event listener"
  []
  (PersistentTracingListener. []))

(defn is-tracing?
  "Returns true if the given session has tracing enabled, false otherwise."
  [session]
  (let [{:keys [listeners]} (eng/components session)]
    (boolean (some #(instance? PersistentTracingListener %) listeners))))

(defn with-tracing
  "Returns a new session identical to the given one, but with tracing enabled.
   The given session is returned unmodified if tracing is already enabled."
  [session]
  (if (is-tracing? session)
    session
    (eng/with-listener session (PersistentTracingListener. []))))

(defn without-tracing
  "Returns a new session identical to the given one, but with tracing disabled
   The given session is returned unmodified if tracing is already disabled."
  [session]
  (eng/remove-listeners session (partial instance? PersistentTracingListener)))

(defn get-trace
  "Returns the trace from the given session."
  [session]
  (if-let [listener (->> (eng/components session)
                         :listeners
                         (filter #(instance? PersistentTracingListener %))
                         (first))]
    (.-trace ^PersistentTracingListener listener)
    (throw (ex-info "No tracing listener attached to session." {:session session}))))

(defn ^:private node-id->productions
  "Given a session and a node ID return a list of the rule and query names associated
   with the node."
  [session id]
  (let [node (-> session eng/components :rulebase :id-to-node (get id))]
    (into []
          (comp
           (map second)
           cat
           (map second))
          (eng/get-conditions-and-rule-names node))))

(defn ranked-productions
  "Given a session with tracing enabled, return a map of rule and query names
  to a numerical index that represents an approximation of the proportional
  amount of times Clara performed processing related to this rule.  This
  is not intended to have a precise meaning, and is intended solely as a means
  to provide a rough guide to which rules and queries should be considered
  the first suspects when diagnosing  performance problems in rules sessions.
  It is possible for a relatively small number of interactions to take a long
  time if those interactions are particularly costly.  It is expected that
  the results may change between different versions when Clara's internals change,
  for example to optimize the rules network.  Nevertheless, it is anticipated
  that this will provide useful information for a first pass at rules
  performance problem debugging.  This should not be used to drive user logic.

  This currently returns a Clojure array map in order to conveniently have the rules
  with the most interactions printed first in the string representation of the map."
  [session]
  (let [node-ids (->> session
                      get-trace
                      (map :node-id))

        production-names (into []
                               (comp
                                (map (partial node-id->productions session))
                                cat)
                               node-ids)

        production-name->interactions (frequencies  production-names)

        ranked-tuples (reverse (sort-by second production-name->interactions))]

    (apply array-map (into [] cat ranked-tuples))))
