(ns clara.tools.loop-detector
  (:require [clara.rules.listener :as l]
            [clara.rules.engine :as eng]
            [clara.tools.tracing :as trace]))

(deftype CyclicalRuleListener [cycles-count max-cycles on-limit-fn]
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
      (on-limit-fn))
    (swap! cycles-count inc))
  (to-persistent! [listener]
    (CyclicalRuleListener. (atom 0) max-cycles on-limit-fn))

  l/IPersistentEventListener
  (to-transient [listener]
    listener))

(defn throw-exception-on-max-cycles
  []
  (let [trace (trace/listener->trace (l/to-persistent! (:listener eng/*current-session*)))]
    (throw (ex-info "Reached maximum activation group transitions threshhold; an infinite loop is suspected"
                    (cond-> {:clara-rules/infinite-loop-suspected true}
                      trace (assoc :trace trace))))))

(defn ->standard-out-warning
  []
  (println "Reached maximum activation group transitions threshhold; an infinite loop is suspected"))

(defn ->invoke-once-fn
  [wrapped-fn]
  (let [warned (atom false)]
    (fn []
      (when-not @warned
        (reset! warned true)
        (wrapped-fn)))))

(defn on-error-fns
  [lookup]
  (case lookup
    :throw-exception throw-exception-on-max-cycles
    :standard-out-warning ->standard-out-warning
    nil))


(defn with-loop-detection
  "Detect suspected infinite loops in the session.  

   Max-cycles is the maximum
   number of transitions permitted between different activation groups (salience levels)
   plus the number of times all rules are evaluated and their facts inserted, thus leading
   to another cycle of rules activations in the same activation group. 

   on-limit-fn is a 0-arg function that is invoked exactly once when this limit is exceeded.  It can either be
   a user-provided function or a keyword that indicates a built-in function to use.  Currently supported keywords are:

   :throw-exception - This throws an exception when the limit is reached.  If tracing is enabled, the exception will include
   the trace.

   :standard-out-warning - This prints a warning to standard out."

  [session max-cycles on-limit-fn]
  
  (eng/with-listener
    session
    (CyclicalRuleListener.
     (atom 0)
     max-cycles
     (->invoke-once-fn (or (on-error-fns on-limit-fn)
                           on-limit-fn)))))
