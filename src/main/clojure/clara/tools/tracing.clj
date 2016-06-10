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

  (insert-facts! [listener facts]
    (append-trace listener {:type :add-facts :facts facts}))

  (insert-facts-logical! [listener node token facts]
    (append-trace listener {:type :add-facts-logical :token token :facts facts}))

  (retract-facts! [listener facts]
    (append-trace listener {:type :retract-facts :facts facts}))

  (retract-facts-logical! [listener node token facts]
    (append-trace listener {:type :retract-facts-logical :token token :facts facts}))

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

  (fire-rules! [listener node]
    (append-trace listener {:type :fire-rules :node-id (:id node)}))

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
    (let [{:keys [listeners] :as components} (eng/components session)]
      (eng/assemble (assoc components
                      :listeners
                      (conj listeners (PersistentTracingListener. [])))))))

(defn without-tracing
  "Returns a new session identical to the given one, but with tracing disabled
   The given session is returned unmodified if tracing is already disabled."
  [session]
  (if (is-tracing? session)
    (let [{:keys [listeners] :as components} (eng/components session)]
      (eng/assemble (assoc components
                      :listeners
                      (remove #(instance? PersistentTracingListener %) listeners))))
    session))

(defn get-trace
  "Returns the trace from the given session."
  [session]
  (if-let [listener (->> (eng/components session)
                         :listeners
                         (filter #(instance? PersistentTracingListener %) )
                         (first))]
    (.-trace ^PersistentTracingListener listener)
    (throw (IllegalStateException. "No tracing listener attached to session."))))
