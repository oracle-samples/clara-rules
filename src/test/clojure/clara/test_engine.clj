(ns clara.test-engine
  (:require [clara.rules :refer [mk-session
                                 fire-rules
                                 fire-rules-async
                                 query
                                 defrule defquery
                                 insert-all
                                 insert!]]
            [clojure.core.async :refer [go timeout <!]]
            [futurama.core :refer [async !<! !<!!]]
            [clojure.test :refer [deftest testing is]]
            [criterium.core :refer [report-result
                                    with-progress-reporting
                                    quick-benchmark]]))

(defrule test-slow-rule-1
  "this rule does some async work using go block"
  [:number [{:keys [value]}]
   (= value ?value)
   (pos? ?value)]
  =>
  (go
    (<! (timeout 50))
    (insert! {:type :result
              :value (+ ?value 100)})))

(defrule test-slow-rule-2
  "this rule does some async work using async block"
  [:result [{:keys [value]}]
   (= value ?value)
   (pos? ?value)]
  =>
  (async
   (!<! (timeout 50))
   (insert! {:type :output
             :value (inc ?value)})))

(defquery test-slow-query
  []
  [:output [{:keys [value]}] (= value ?value)])

(def session-50
  (let [fact-seq (repeat 50 {:type :number
                             :value 199})
        session (-> (mk-session 'clara.test-engine :fact-type-fn :type :cache false)
                    (insert-all fact-seq))]
    session))

(def session-10000
  (let [fact-seq (repeat 10000 {:type :number
                                :value 199})
        session (-> (mk-session 'clara.test-engine :fact-type-fn :type)
                    (insert-all fact-seq))]
    session))

(deftest parallel-compute-engine-performance-test
  (testing "parallel compute with large batch size for non-blocking io - 50 facts - 100 batch size"
    (let [result (with-progress-reporting
                   (quick-benchmark
                    (-> (!<!! (fire-rules-async session-50 {:parallel-batch-size 100}))
                        (query test-slow-query)
                        (count))
                    {:verbose true}))
          [mean [lower upper]] (:mean result)]
      (is (<= 0.1 lower mean upper 0.2)) ;;; our lower and mean values should be between 100ms and 200ms
      (report-result result)))
  (testing "parallel compute with large batch size for non-blocking io - 10000 facts - 20000 batch size"
    (let [result (with-progress-reporting
                   (quick-benchmark
                    (-> (!<!! (fire-rules-async session-10000 {:parallel-batch-size 20000}))
                        (query test-slow-query)
                        (count))
                    {:verbose true}))
          [mean [lower upper]] (:mean result)]
      (is (<= 0.1 lower mean upper 1.0)) ;;; our lower and mean values should be between 100ms and 1000ms
      (report-result result))))
