(ns clara.tools.internal.inspect
  (:require [clara.rules.listener :as l]
            [clara.rules.engine :as eng]))

(declare to-persistent-listener)

(deftype TransientRuleActivationListener [activations]
  l/ITransientEventListener
  (fire-activation! [listener activation resulting-operations]
    (reset! (.-activations listener) (conj @(.-activations listener) {:activation activation
                                                                      :resulting-operations resulting-operations}))
    listener)
  (to-persistent! [listener]
    (to-persistent-listener @(.-activations listener)))

  ;; The methods below don't do anything; they aren't needed for this functionality.
  (left-activate! [listener node tokens]
    listener)
  (left-retract! [listener node tokens]
    listener)
  (right-activate! [listener node elements]
    listener)
  (right-retract! [listener node elements]
    listener)
  (insert-facts! [listener node token facts]
    listener)
  (alpha-activate! [listener node facts]
    listener)
  (insert-facts-logical! [listener node token facts]
    listener)
  (retract-facts! [listener node token facts]
    listener)
  (alpha-retract! [listener node facts]
    listener)
  (retract-facts-logical! [listener node token facts]
    listener)
  (add-accum-reduced! [listener node join-bindings result fact-bindings]
    listener)
  (remove-accum-reduced! [listener node join-bindings fact-bindings]
    listener)
  (add-activations! [listener node activations]
    listener)
  (remove-activations! [listener node activations]
    listener)
  (fire-rules! [listener node]
    listener))

(deftype PersistentRuleActivationListener [activations]
  l/IPersistentEventListener
  (to-transient [listener]
    (TransientRuleActivationListener. (atom activations))))

(defn to-persistent-listener
  [activations]
  (PersistentRuleActivationListener. activations))

(defn with-rule-activation-listening
  [session]
  (if (empty? (eng/listeners-matching-pred session (partial instance? PersistentRuleActivationListener)))
    (eng/with-listener session (PersistentRuleActivationListener. []))
    session))

(defn without-rule-activation-listening
  [session]
  (eng/remove-listeners session (partial instance? PersistentRuleActivationListener)))

(defn get-activation-info
  [session]
  (let [matching-listeners (eng/listeners-matching-pred session (partial instance? PersistentRuleActivationListener))]
    (condp = (count matching-listeners)
      0 nil
      1 (-> matching-listeners ^PersistentRuleActivationListener (first) .activations)
      (throw (ex-info "Found more than one PersistentRuleActivationListener on session"
                      {:session session})))))

  
