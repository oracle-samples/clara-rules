(ns clara.test-rules
  (:require-macros [cemerick.cljs.test :refer (is deftest with-test run-tests testing test-var)]
                   [clara.rules :refer [mk-rule]])
  (:require [cemerick.cljs.test :as t]
            [clara.rules :refer [mk-rulebase mk-session insert fire-rules]]
            [clara.rules.engine :refer [->Token ast-to-dnf load-rules *trace-transport*  description]]

            ;; TODO: need to fix typing issues in ClojureScript port before using records.
            [clara.rules.testfacts :refer [->Temperature Temperature]]))

(deftest test-temperature-map
  
  (binding [*trace-transport* true]
    (let [rule-output (atom nil)
          cold-rule (mk-rule [[:temperature [{temperature :temperature}] (< temperature 20)]]
                             (reset! rule-output ?__token__) )

          session (-> (mk-rulebase cold-rule)
                      (mk-session :fact-type-fn :type)
                      (insert {:type :temperature :temperature 10 :location "MCI"})
                      (fire-rules))]

      (is (= 
           (->Token [{:type :temperature :temperature 10 :location "MCI"}] {})
           @rule-output)))))