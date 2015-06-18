(ns clara.rules.engine.helpers
  (:require
    [clara.rules.engine.impl :as impl]
    [clara.rules.engine.wme :as wme]))

(defn has-keys?
  "Returns true if the given map has all of the given keys."
  [m keys]
  (every? (partial contains? m) keys))

(defn matches-some-facts?
  "Returns true if the given token matches one or more of the given elements."
  [token elements join-filter-fn condition]
  (some (fn [{:keys [fact]}]
          (join-filter-fn token fact (:env condition)))
        elements))

(defn send-accumulated
  "Helper function to send the result of an accumulated value to the node's children."
  [node accum-condition accumulator result-binding token result fact-bindings transport memory listener]
  (let [converted-result ((:convert-return-fn accumulator) result)
        new-bindings (merge (:bindings token)
                            fact-bindings
                            (when result-binding
                              { result-binding
                                converted-result}))]

    (impl/send-tokens transport memory listener (:children node)
                 [(wme/->Token (conj (:matches token) [converted-result (:id node)]) new-bindings)])))


(defn flush-updates
  "Flush pending updates in the current session. Returns true if there were some items to flush,
  false otherwise"
  [current-session]

  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} current-session
        pending-updates @(:pending-updates current-session)]

    ;; Remove the facts here so they are re-inserted if we flush recursively.
    (reset! (:pending-updates current-session) [])

    (doseq [partition (partition-by :type pending-updates)
            :let [facts (mapcat :facts partition)]
            [alpha-roots fact-group] (get-alphas-fn facts)
            root alpha-roots]

      (if (= :insert (:type (first partition)))
        (impl/alpha-activate root fact-group transient-memory transport listener)
        (impl/alpha-retract root fact-group transient-memory transport listener)))

    (not (empty? pending-updates))))

(defn retract-accumulated
  "Helper function to retract an accumulated value."
  [node accum-condition accumulator result-binding token result fact-bindings transport memory listener]
  (let [converted-result ((:convert-return-fn accumulator) result)
        new-facts (conj (:matches token) [converted-result (:id node)])
        new-bindings (merge (:bindings token)
                            fact-bindings
                            (when result-binding
                              { result-binding
                                converted-result}))]

    (impl/retract-tokens transport memory listener (:children node)
                    [(wme/->Token new-facts new-bindings)])))