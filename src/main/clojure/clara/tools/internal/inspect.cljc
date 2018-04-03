(ns clara.tools.internal.inspect
  "Internal implementation details of session inspection.  Nothing in this namespace
   should be directly referenced by callers outside of the clara-rules project."
  (:require [clara.rules.listener :as l]
            [clara.rules.engine :as eng]))

(declare to-persistent-listener)

(deftype TransientActivationListener [activations]
  l/ITransientEventListener
  (fire-activation! [listener activation resulting-operations]
    (swap! (.-activations listener) conj {:activation activation
                                          :resulting-operations resulting-operations})
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

(deftype PersistentActivationListener [activations]
  l/IPersistentEventListener
  (to-transient [listener]
    (TransientActivationListener. (atom activations))))

(defn to-persistent-listener
  [activations]
  (PersistentActivationListener. activations))

(defn with-activation-listening
  [session]
  (if (empty? (eng/find-listeners session (partial instance? PersistentActivationListener)))
    (eng/with-listener session (PersistentActivationListener. []))
    session))

(defn without-activation-listening
  [session]
  (eng/remove-listeners session (partial instance? PersistentActivationListener)))

(defn get-activation-info
  [session]
  (let [matching-listeners (eng/find-listeners session (partial instance? PersistentActivationListener))]
    (condp = (count matching-listeners)
      0 nil
      1 (-> matching-listeners ^PersistentActivationListener (first) .-activations)
      (throw (ex-info "Found more than one PersistentActivationListener on session"
                      {:session session})))))

  
