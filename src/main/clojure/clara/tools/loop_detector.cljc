(ns clara.tools.loop-detector
  (:require [clara.rules.listener :as l]
            [clara.rules.engine :as eng]
            [clara.tools.tracing :as trace]))

;; Although we use a single type here note that the cycles-count and the on-limit-delay fields
;; will be nil during the persistent state of the listener.
(deftype CyclicalRuleListener [cycles-count max-cycles on-limit-fn on-limit-delay]
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
      @on-limit-delay)
    (swap! cycles-count inc))
  (to-persistent! [listener]
    (CyclicalRuleListener. nil max-cycles on-limit-fn nil))

  l/IPersistentEventListener
  (to-transient [listener]
    ;; To-transient will be called when a call to fire-rules begins, and to-persistent! will be called when it ends.
    ;; The resetting of the cycles-count atom prevents cycles from one call of fire-rules from leaking into the count
    ;; for another.  Similarly the on-limit-fn should be invoked 1 or 0 times per fire-rules call. We only call
    ;; it once, rather than each time the limit is breached, since it may not cause the call to terminate but rather log
    ;; something etc., in which case we don't want to spam the user's logs.
    (CyclicalRuleListener. (atom 0) max-cycles on-limit-fn (delay (on-limit-fn)))))

(defn throw-exception-on-max-cycles
  []
  (let [trace (trace/listener->trace (l/to-persistent! (:listener eng/*current-session*)))]
    (throw (ex-info "Reached maximum activation group transitions threshhold; an infinite loop is suspected"
                    (cond-> {:clara-rules/infinite-loop-suspected true}
                      trace (assoc :trace trace))))))

(defn ->standard-out-warning
  []
  (println "Reached maximum activation group transitions threshhold; an infinite loop is suspected"))

(defn on-limit-fn-lookup
  [fn-or-keyword]
  (cond
    (= fn-or-keyword :throw-exception) throw-exception-on-max-cycles
    (= fn-or-keyword :standard-out-warning) ->standard-out-warning
    (ifn? fn-or-keyword) fn-or-keyword
    :else (throw (ex-info "The :on-error-fn must be a non-nil function value" {:clara-rules/max-cycles-exceeded-fn fn-or-keyword}))))


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

  (let [on-limit-fn-normalized (on-limit-fn-lookup on-limit-fn)]
    (eng/with-listener
      session
      (CyclicalRuleListener.
       nil
       max-cycles
       on-limit-fn-normalized
       nil))))
