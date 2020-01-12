(ns clara.tools.loop-detector
  (:require [clara.rules.listener :as l]
            [clara.rules.engine :as eng]))

(deftype LoopDetectorListener [cycles-count max-cycles fn-on-limit]
    l/ITransientEventListener
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
    (when (>= @cycles-count max-cycles)
      (fn-on-limit))
    (swap! cycles-count inc))
  (to-persistent! [listener]
    (LoopDetectorListener. (atom 0) max-cycles fn-on-limit))

  l/IPersistentEventListener
  (to-transient [listener]
    listener))

(defn with-loop-detection [session fn-on-limit max-cycles]
  (eng/with-listener session (LoopDetectorListener. (atom 0) max-cycles fn-on-limit)))
