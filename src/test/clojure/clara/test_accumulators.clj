(ns clara.test-accumulators
  (:use clojure.test
        clara.rules
        clojure.pprint
        [clara.rules.engine :only [->Token ast-to-dnf load-rules *trace-transport* 
                                   description print-memory]]
        clara.rules.testfacts)
  (:require [clara.sample-ruleset :as sample]
            [clojure.set :as s]
            [clara.rules.accumulators :as acc])
  (import [clara.rules.testfacts Temperature WindSpeed Cold ColdAndWindy LousyWeather First Second Third Fourth]))

(deftest test-max
  (let [hottest (mk-query [] [[?t <- (acc/max :temperature) from [Temperature]]])

        session (-> (mk-rulebase hottest) 
                    (mk-session)
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    (is (= {:?t 80} (first (query session hottest))))))


(deftest test-min-max-average
  (let [coldest (mk-query [] [[?t <- (acc/min :temperature) from [Temperature]]])
        coldest-fact (mk-query [] [[?t <- (acc/min :temperature :returns-fact true) from [Temperature]]])

        hottest (mk-query [] [[?t <- (acc/max :temperature) from [Temperature]]])
        hottest-fact (mk-query [] [[?t <- (acc/max :temperature :returns-fact true) from [Temperature]]])

        average-temp (mk-query [] [[?t <- (acc/average :temperature) from [Temperature]]])

        session (-> (mk-rulebase coldest coldest-fact hottest hottest-fact average-temp) 
                    (mk-session)
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    (is (= {:?t 10} (first (query session coldest))))
    (is (= #{{:?t (->Temperature 10 "MCI")}}
           (set (query session coldest-fact))))

    (is (= {:?t 80} (first (query session hottest))))   
    (is (= #{{:?t (->Temperature 80 "MCI")}}
           (set (query session hottest-fact))))

    (is (= (list {:?t 40}) (query session average-temp)))))

(deftest test-sum 
  (let [sum (mk-query [] [[?t <- (acc/sum :temperature) from [Temperature]]])

        session (-> (mk-rulebase) 
                    (add-productions sum)                 
                    (mk-session)
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    (is (= {:?t 120} (first (query session sum))))))

(deftest test-count
  (let [count (mk-query [] [[?c <- (acc/count) from [Temperature]]])

        session (-> (mk-rulebase count) 
                    (mk-session)
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    (is (= {:?c 3} (first (query session count))))))

(deftest test-distinct 
  (let [distinct (mk-query [] [[?t <- (acc/distinct) from [Temperature]]])
        distinct-field (mk-query [] [[?t <- (acc/distinct :temperature) from [Temperature]]])

        session (-> (mk-rulebase distinct distinct-field) 
                    (mk-session)
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 90 "MCI")))]

    (is (= #{{:?t #{ (->Temperature 80 "MCI")  
                     (->Temperature 90 "MCI")}}}
           (set (query session distinct))))

    (comment (is (= #{{:?t #{ 80 90}}}
                    (set (query session distinct-field)))))))

(deftest test-max-min-avg
  ;; Tests a single query that gets the maximum, minimum, and average temperatures.
  (let [max-min-avg (mk-query [] [[?max <- (acc/max :temperature) from [Temperature]]
                                  [?min <- (acc/min :temperature) from [Temperature]]
                                  [?avg <- (acc/average :temperature) from [Temperature]]])

        session (-> (mk-rulebase max-min-avg) 
                    (mk-session)
                    (insert (->Temperature 30 "MCI")
                            (->Temperature 10 "MCI")
                            (->Temperature 80 "MCI")))]

    (is (= {:?max 80 :?min 10 :?avg 40} (first (query session max-min-avg))))))


(deftest test-count-none
  (let [count (mk-query [] [[?c <- (acc/count) from [Temperature]]])

        session (-> (mk-rulebase count) 
                    (mk-session))]

    (is (= {:?c 0} (first (query session count))))))

(deftest test-count-none-joined
  (let [count (mk-query [] [[WindSpeed (> windspeed 10) (= ?loc location)]
                            [?c <- (acc/count) from [Temperature (= ?loc location)]]])

        session (-> (mk-rulebase count)
                    (mk-session)
                    (insert (->WindSpeed 20 "MCI")))]

    (is (= {:?c 0 :?loc "MCI"} (first (query session count))))))

;; Same as the above test, but the binding occurs in a rule after
;; the accumulator, to test reordering.
(deftest test-count-none-with-later-bind
  (let [count (mk-query [] [[?c <- (acc/count) from [Temperature (= ?loc location)]]
                            [WindSpeed (> windspeed 10) (= ?loc location)]])

        session (-> (mk-rulebase count)
                    (mk-session)
                    (insert (->WindSpeed 20 "MCI")))]

    (is (= {:?c 0 :?loc "MCI"} (first (query session count))))))


(deftest test-count-some-empty
  (let [count (mk-query [:?loc] [[?c <- (acc/count) from [Temperature (= ?loc location)]]
                                [WindSpeed (> windspeed 10) (= ?loc location)]])

        session (-> (mk-rulebase count)
                    (mk-session)
                    (insert (->WindSpeed 20 "MCI")
                            (->WindSpeed 20 "SFO")
                            (->Temperature 40 "SFO")
                            (->Temperature 50 "SFO")))]
    
    ;; Zero count at MCI, since no temperatures were inserted.
    (is (= {:?c 0 :?loc "MCI"} (first (query session count :?loc "MCI"))))

    ;; Ensure the two temperature readings at SFO are found as expected.
    (is (= {:?c 2 :?loc "SFO"} (first (query session count :?loc "SFO"))))))
