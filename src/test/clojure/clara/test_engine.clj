(ns clara.test-engine
  (:require [clara.rules :refer [clear-ns-productions!
                                 mk-session
                                 fire-rules
                                 query
                                 defrule defquery
                                 insert-all insert-all!
                                 insert insert!
                                 retract retract!]]
            [clara.rules.compiler :refer [clear-session-cache!]]
            [clara.rules.platform :as platform]
            [clara.rules.engine :as eng]
            [criterium.core :refer [report-result
                                    quick-benchmark]]))
(defrule test-slow-rule-1
  [:number [{:keys [value]}]
   (= value ?value)
   (do (Thread/sleep 50) (pos? ?value))]
  =>
  (println "number:" ?value)
  (insert! {:type :result
            :value (+ ?value 100)}))

(defrule test-slow-rule-2
  [:result [{:keys [value]}]
   (= value ?value)
   (do (Thread/sleep 50) (pos? ?value))]
  =>
  (println "result:" ?value)
  (Thread/sleep 50)
  (insert! {:type :output
            :value (inc ?value)}))

(defquery test-slow-query
  []
  [:output [{:keys [value]}] (= value ?value)])

(def session
  (let [fact-seq (repeat 50 {:type :number
                             :value 199})
        session (-> (mk-session 'clara.test-engine :fact-type-fn :type)
                    (insert-all fact-seq))]
    session))

(comment
  (time
   (-> (fire-rules session {:parallel-compute true})
       (query test-slow-query)
       (count)))
  (do
    (clear-ns-productions!)
    (clear-session-cache!)))
