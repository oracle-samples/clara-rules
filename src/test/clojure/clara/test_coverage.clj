(ns ^:coverage clara.test-coverage
  (:require [clojure.test :refer [deftest testing is]]
            [clara.rules :refer [mk-session insert fire-rules query]]
            [clara.coverage-ruleset]))

(deftest test-coverage
  (testing "run the rules"
    (let [result (-> (mk-session 'clara.coverage-ruleset :fact-type-fn :type)
                     (insert {:type :weather
                              :temperature 75})
                     (fire-rules)
                     (query clara.coverage-ruleset/climate-query))]
      (is (= result [{:?result {:type :climate
                                :label "Warm"}}])))))
