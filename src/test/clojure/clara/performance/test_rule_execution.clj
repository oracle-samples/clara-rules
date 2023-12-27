(ns clara.performance.test-rule-execution
  (:require [clara.rules.accumulators :as acc]
            [clara.rules :as r]
            [clojure.test :refer :all]
            [clara.tools.testing-utils :refer [def-rules-test run-performance-test]]))

(defrecord AFact [id])
(defrecord BFact [id])
(defrecord ParentFact [a-id b-id])

(def counter (atom {:a-count 0
                    :b-count 0}))

(def number-of-facts 1500)

(def-rules-test test-get-in-perf
  {:rules [rule [[[?parent <- ParentFact]
                  [?as <- (acc/all) :from [AFact (= (:a-id ?parent) id)]]
                  [?bs <- (acc/all) :from [BFact (= (:b-id ?parent) id)]]]
                 '(do (swap! counter update :a-count inc))]]
   :sessions [session [rule] {}]}
  (let [parents (for [x (range number-of-facts)]
                  (->ParentFact x (inc x)))
        a-facts (for [id (range number-of-facts)]
                  (->AFact id))
        b-facts (for [id (range number-of-facts)]
                  (->BFact id))

        facts (doall (concat parents
                             a-facts
                             b-facts))]
    (run-performance-test {:description "Slow get-in perf"
                           :func #(-> session
                                      (r/insert-all facts)
                                      r/fire-rules)
                           :iterations 5
                           :mean-assertion (partial > 10000)})))
