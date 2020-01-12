(ns clara.rules.listener
  "Event listeners for analyzing the flow through Clara. This is for primarily for use by
   tooling, but advanced users may use this to analyze sessions.")

(defprotocol IPersistentEventListener
  (to-transient [listener]))

;; TODO: Handle add-accum-reduced
(defprotocol ITransientEventListener
  (left-activate! [listener node tokens])
  (left-retract! [listener node tokens])
  (right-activate! [listener node elements])
  (right-retract! [listener node elements])
  (insert-facts! [listener node token facts])
  (alpha-activate! [listener node facts])
  (insert-facts-logical! [listener node token facts])
  (retract-facts! [listener node token facts])
  (alpha-retract! [listener node facts])
  (retract-facts-logical! [listener node token facts])
  (add-accum-reduced! [listener node join-bindings result fact-bindings])
  (remove-accum-reduced! [listener node join-bindings fact-bindings])
  (add-activations! [listener node activations])
  (remove-activations! [listener node activations])
  (fire-activation! [listener activation resulting-operations])
  (fire-rules! [listener node])
  (activation-group-transition! [listener original-group new-group])
  (to-persistent! [listener]))

;; A listener that does nothing.
(deftype NullListener []
  ITransientEventListener
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
  (fire-activation! [listener activation resulting-operations]
    listener)
  (fire-rules! [listener node]
    listener)
  (activation-group-transition! [listener original-group new-group]
    listener)
  (to-persistent! [listener]
    listener)

  IPersistentEventListener
  (to-transient [listener]
    listener))

(declare delegating-listener)

;; A listener that simply delegates to others
(deftype DelegatingListener [children]
  ITransientEventListener
  (left-activate! [listener node tokens]
    (doseq [child children]
      (left-activate! child node tokens)))

  (left-retract! [listener node tokens]
    (doseq [child children]
      (left-retract! child node tokens)))

  (right-activate! [listener node elements]
    (doseq [child children]
      (right-activate! child node elements)))

  (right-retract! [listener node elements]
    (doseq [child children]
      (right-retract! child node elements)))

  (insert-facts! [listener node token facts]
    (doseq [child children]
      (insert-facts! child node token facts)))
  
  (alpha-activate! [listener node facts]
    (doseq [child children]
      (alpha-activate! child node facts)))

  (insert-facts-logical! [listener node token facts]
    (doseq [child children]
      (insert-facts-logical! child node token facts)))

  (retract-facts! [listener node token facts]
    (doseq [child children]
      (retract-facts! child node token facts)))
  
  (alpha-retract! [listener node facts]
    (doseq [child children]
      (alpha-retract! child node facts)))

  (retract-facts-logical! [listener node token facts]
    (doseq [child children]
      (retract-facts-logical! child node token facts)))

  (add-accum-reduced! [listener node join-bindings result fact-bindings]
    (doseq [child children]
      (add-accum-reduced! child node join-bindings result fact-bindings)))

  (remove-accum-reduced! [listener node join-bindings fact-bindings]
    (doseq [child children]
      (remove-accum-reduced! child node join-bindings fact-bindings)))

  (add-activations! [listener node activations]
    (doseq [child children]
      (add-activations! child node activations)))

  (remove-activations! [listener node activations]
    (doseq [child children]
      (remove-activations! child node activations)))

  (fire-activation! [listener activation resulting-operations]
    (doseq [child children]
      (fire-activation! child activation resulting-operations)))

  (fire-rules! [listener node]
    (doseq [child children]
      (fire-rules! child node)))

  (activation-group-transition! [listener original-group new-group]
    (doseq [child children]
      (activation-group-transition! child original-group new-group)))

  (to-persistent! [listener]
    (delegating-listener (map to-persistent! children))))

(deftype PersistentDelegatingListener [children]
  IPersistentEventListener
  (to-transient [listener]
    (DelegatingListener. (map to-transient children))))

(defn delegating-listener
  "Returns a listener that delegates to its children."
  [children]
  (PersistentDelegatingListener. children))

(defn null-listener?
  "Returns true if the given listener is the null listener, false otherwise."
  [listener]
  (instance? NullListener listener))

(defn get-children
  "Returns the children of a delegating listener."
  [^PersistentDelegatingListener listener]
  (.-children listener))

;; Default listener.
(def default-listener (NullListener.))
