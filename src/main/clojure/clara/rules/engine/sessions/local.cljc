(ns clara.rules.engine.sessions.local
  (:require
    [clara.rules.engine.protocols :as impl] [clara.rules.memory :as mem]
    [clara.rules.engine.helpers :as hlp] [clara.rules.platform :as platform]
    [clara.rules.engine.state :as state] [clara.rules.listener :as l]))

(defn fire-rules*
  "Fire rules for the given nodes."
  [rulebase nodes transient-memory transport listener get-alphas-fn]
  (binding [state/*current-session* {:rulebase rulebase
                                     :transient-memory transient-memory
                                     :transport transport
                                     :insertions (atom 0)
                                     :get-alphas-fn get-alphas-fn
                                     :pending-updates (atom [])
                                     :listener listener}]
    (loop [next-group (mem/next-activation-group transient-memory)
           last-group nil]
      (if next-group
        (if (and last-group (not= last-group next-group))
          ;; We have changed groups, so flush the updates from the previous
          ;; group before continuing.
          (do
            (hlp/flush-updates state/*current-session*)
            (recur (mem/next-activation-group transient-memory) next-group))
          (do
            ;; If there are activations, fire them.
            (when-let [{:keys [node token]} (mem/pop-activation! transient-memory)]
              (binding [state/*rule-context* {:token token :node node}]

                ;; Fire the rule itself.
                ((:rhs node) token (:env (:production node)))

                ;; Explicitly flush updates if we are in a no-loop rule, so the no-loop
                ;; will be in context for child rules.
                (when (some-> node :production :props :no-loop)
                  (hlp/flush-updates state/*current-session*))))
            (recur (mem/next-activation-group transient-memory) next-group)))

        ;; There were no items to be activated, so flush any pending
        ;; updates and recur with a potential new activation group
        ;; since a flushed item may have triggered one.
        (when (hlp/flush-updates state/*current-session*)
          (recur (mem/next-activation-group transient-memory) next-group))))))

(deftype LocalSession [rulebase memory transport listener get-alphas-fn]
  impl/ISession
  (insert [session facts]
    (let [transient-memory (mem/to-transient memory)
          transient-listener (l/to-transient listener)]
      (l/insert-facts! transient-listener facts)
      (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
              root alpha-roots]
        (impl/alpha-activate root fact-group transient-memory transport transient-listener))
      (LocalSession. rulebase
                     (mem/to-persistent! transient-memory)
                     transport
                     (l/to-persistent! transient-listener)
                     get-alphas-fn)))

  (retract [session facts]

    (let [transient-memory (mem/to-transient memory)
          transient-listener (l/to-transient listener)]

      (l/retract-facts! transient-listener facts)

      (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
              root alpha-roots]
        (impl/alpha-retract root fact-group transient-memory transport transient-listener))

      (LocalSession. rulebase
                     (mem/to-persistent! transient-memory)
                     transport
                     (l/to-persistent! transient-listener)
                     get-alphas-fn)))

  (fire-rules [session]
    (let [transient-memory (mem/to-transient memory)
          transient-listener (l/to-transient listener)]
      (fire-rules* rulebase
                   (:production-nodes rulebase)
                   transient-memory
                   transport
                   transient-listener
                   get-alphas-fn)

      (LocalSession. rulebase
                     (mem/to-persistent! transient-memory)
                     transport
                     (l/to-persistent! transient-listener)
                     get-alphas-fn)))

  ;; TODO: queries shouldn't require the use of transient memory.
  (query [session query params]
    (let [query-node (get-in rulebase [:query-nodes query])]
      (when (= nil query-node)
        (platform/throw-error (str "The query " query " is invalid or not included in the rule base.")))

      (->> (mem/get-tokens (mem/to-transient memory) query-node params)

           ;; Get the bindings for each token and filter generate symbols.
           (map (fn [{bindings :bindings}]

                  ;; Filter generated symbols. We check first since this is an uncommon flow.
                  (if (some #(re-find #"__gen" (name %)) (keys bindings) )

                    (into {} (remove (fn [[k v]] (re-find #"__gen"  (name k)))
                                     bindings))
                    bindings))))))

  (components [session]
    {:rulebase rulebase
     :memory memory
     :transport transport
     :listeners (if (l/null-listener? listener)
                  []
                  (l/get-children listener))
     :get-alphas-fn get-alphas-fn}))