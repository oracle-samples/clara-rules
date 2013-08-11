(ns clara.test-accumulators
  (:use clojure.test
        clara.rules
        clojure.pprint
        [clara.rules.engine :only [->Token ast-to-dnf load-rules *trace-transport* 
                                   description]]
        clara.rules.testfacts)
  (:refer-clojure :exclude [==])
  (:require [clara.sample-ruleset :as sample]
            [clojure.set :as s]
            [clara.rules.accumulators :as acc])
  (import [clara.rules.testfacts Temperature WindSpeed Cold ColdAndWindy LousyWeather First Second Third Fourth]))


(deftest test-min-max-average
  (let [coldest (mk-query [] [[?t <- (acc/min :temperature) from [Temperature]]])

        hottest (mk-query [] [[?t <- (acc/max :temperature) from [Temperature]]])
        hottest-fact (mk-query [] [[?t <- (acc/max :temperature :returns-fact true) from [Temperature]]])

        average-temp (mk-query [] [[?t <- (acc/average :temperature) from [Temperature]]])

        session (-> (mk-rulebase) 
                    (add-query coldest)
                    (add-query hottest)
                    (add-query hottest-fact)
                    (add-query average-temp)                 
                    (mk-session)
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    (is (= {:?t 10} (first (query session coldest {}))))
  

    (is (= {:?t 80} (first (query session hottest {}))))
    
    (is (= #{{:?t (->Temperature 80 "MCI")}}
           (set (query session hottest-fact {}))))

    (is (= (list {:?t 40}) (query session average-temp {})))))

(deftest test-sum 
  (let [sum (mk-query [] [[?t <- (acc/sum :temperature) from [Temperature]]])

        session (-> (mk-rulebase) 
                    (add-query sum)                 
                    (mk-session)
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    (is (= {:?t 120} (first (query session sum {}))))))

(deftest test-count
  (let [count (mk-query [] [[?c <- (acc/count) from [Temperature]]])

        session (-> (mk-rulebase) 
                    (add-query count)                 
                    (mk-session)
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    (is (= {:?c 3} (first (query session count {}))))))

(deftest test-distinct 
  (let [distinct (mk-query [] [[?t <- (acc/distinct) from [Temperature]]])
        distinct-field (mk-query [] [[?t <- (acc/distinct :temperature) from [Temperature]]])

        session (-> (mk-rulebase) 
                    (add-query distinct)                 
                    (add-query distinct-field)    
                    (mk-session)
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 90 "MCI")))]

    (is (= #{{:?t #{ (->Temperature 80 "MCI")  
                     (->Temperature 90 "MCI")}}}
           (set (query session distinct {}))))

    (is (= #{{:?t #{ 80 90}}}
           (set (query session distinct-field {}))))))

(run-tests)
