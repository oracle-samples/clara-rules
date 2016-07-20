(ns clara.test-accumulators
  (:use clojure.test
        clara.rules
        clara.rules.testfacts)
  (:require [clara.sample-ruleset :as sample]
            [clojure.set :as s]
            [clara.rules.accumulators :as acc]
            [clara.rules.dsl :as dsl])
  (import [clara.rules.testfacts Temperature WindSpeed Cold ColdAndWindy
           TemperatureHistory LousyWeather First Second Third Fourth]))

(deftest test-max
  (let [hottest (dsl/parse-query [] [[?t <- (acc/max :temperature) from [Temperature]]])

        session (-> (mk-session [hottest])
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)
        session-with-retract (-> session
                                 (insert (->Temperature 100 "MCI"))
                                 (retract (->Temperature 100 "MCI"))
                                 fire-rules)]

    (is (= {:?t 80}
           (first (query session hottest))
           (first (query session-with-retract hottest))))))

(deftest test-max-min-retract-to-nothing
  (doseq [accum [(acc/min :temperature) (acc/max :temperature)]
          :let [cold-temp (dsl/parse-rule [[?t <- accum :from [Temperature]]]
                                          (insert! (->Cold ?t)))
                cold-query (dsl/parse-query [] [[Cold (= ?temp temperature)]])

                with-cold (-> (mk-session [cold-temp cold-query] :cache false)
                              (insert (->Temperature 10 "MCI"))
                              fire-rules)]]
    (is (= (query with-cold cold-query)
           [{:?temp 10}])
        "Without retraction there should be a Cold fact with a temperature of 10")
    (is (= (-> with-cold
               (retract (->Temperature 10 "MCI"))
               fire-rules
               (query cold-query))
           [])
        "When the original temperature is retracted and we go back to having no matching facts the downstream facts should be retracted.")))

(deftest test-min-max-average
  (let [coldest  (dsl/parse-query [] [[?t <- (acc/min :temperature) :from [Temperature]]])
        coldest-fact (dsl/parse-query [] [[?t <- (acc/min :temperature :returns-fact true) from [Temperature]]])

        hottest (dsl/parse-query [] [[?t <- (acc/max :temperature) from [Temperature]]])
        hottest-fact (dsl/parse-query [] [[?t <- (acc/max :temperature :returns-fact true) from [Temperature]]])

        average-temp (dsl/parse-query [] [[?t <- (acc/average :temperature) from [Temperature]]])

        empty-session (-> (mk-session [coldest coldest-fact hottest hottest-fact average-temp]))

        session (-> empty-session
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))

        min-retracted (retract session (->Temperature 10 "MCI"))
        max-retracted (retract session (->Temperature 80 "MCI"))]

    (is (empty (query empty-session coldest)))
    (is (= {:?t 10} (first (query session coldest))))
    (is (= #{{:?t (->Temperature 10 "MCI")}}
           (set (query session coldest-fact))))

    (is (empty (query empty-session hottest)))
    (is (= {:?t 80} (first (query session hottest))))


    (is (= #{{:?t (->Temperature 80 "MCI")}}
           (set (query session hottest-fact))))

    (is (empty (query empty-session average-temp)))
    (is (= (list {:?t 40}) (query session average-temp)))
    (is (= (list {:?t 20}) (query max-retracted average-temp)))))

(deftest test-sum
  (let [sum (dsl/parse-query [] [[?t <- (acc/sum :temperature) from [Temperature]]])

        session (-> (mk-session [sum])
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))

        retracted (retract session (->Temperature 30 "MCI"))]

    (is (= {:?t 120} (first (query session sum))))
    (is (= {:?t 90} (first (query retracted sum))))))

(deftest test-count
  (let [count (dsl/parse-query [] [[?c <- (acc/count) from [Temperature]]])

        session (-> (mk-session [count])
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))

        retracted (retract session (->Temperature 30 "MCI"))]

    (is (= {:?c 3} (first (query session count))))
    (is (= {:?c 2} (first (query retracted count))))))

(deftest test-distinct
  (let [distinct (dsl/parse-query [] [[?t <- (acc/distinct) from [Temperature]]])
        distinct-field (dsl/parse-query [] [[?t <- (acc/distinct :temperature) from [Temperature]]])

        session (-> (mk-session [distinct distinct-field])
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 90 "MCI")))

        retracted (retract session (->Temperature 90 "MCI"))]

    (is (= #{{:?t #{ (->Temperature 80 "MCI")
                     (->Temperature 90 "MCI")}}}
           (set (query session distinct))))

    (is (= #{{:?t #{ (->Temperature 80 "MCI")}}}
           (set (query retracted distinct))))

    (is (= #{{:?t #{ 80 90}}}
           (set (query session distinct-field))))

    (is (= #{{:?t #{ 80}}}
           (set (query retracted distinct-field))))))

(deftest test-max-min-avg
  ;; Tests a single query that gets the maximum, minimum, and average temperatures.
  (let [max-min-avg (dsl/parse-query [] [[?max <- (acc/max :temperature) from [Temperature]]
                                  [?min <- (acc/min :temperature) from [Temperature]]
                                  [?avg <- (acc/average :temperature) from [Temperature]]])

        session (-> (mk-session [max-min-avg])
                    (insert (->Temperature 30 "MCI")
                            (->Temperature 10 "MCI")
                            (->Temperature 80 "MCI")))]

    (is (= {:?max 80 :?min 10 :?avg 40} (first (query session max-min-avg))))))


(deftest test-count-none
  (let [count (dsl/parse-query [] [[?c <- (acc/count) from [Temperature]]])

        session (mk-session [count])]

    (is (= {:?c 0} (first (query session count))))))

(deftest test-count-none-joined
  (let [count (dsl/parse-query [] [[WindSpeed (> windspeed 10) (= ?loc location)]
                            [?c <- (acc/count) from [Temperature (= ?loc location)]]])

        session (-> (mk-session [count])
                    (insert (->WindSpeed 20 "MCI")))]

    (is (= {:?c 0 :?loc "MCI"} (first (query session count))))))



;; Same as the above test, but the binding occurs in a rule after
;; the accumulator, to test reordering.
(deftest test-count-none-with-later-bind
  (let [count (dsl/parse-query [] [[?c <- (acc/count) from [Temperature (= ?loc location)]]
                            [WindSpeed (> windspeed 10) (= ?loc location)]])

        session (-> (mk-session [count])
                    (insert (->WindSpeed 20 "MCI")))]

    (is (= {:?c 0 :?loc "MCI"} (first (query session count))))))


(deftest test-count-some-empty
  (let [count (dsl/parse-query [:?loc] [[?c <- (acc/count) from [Temperature (= ?loc location)]]
                                 [WindSpeed (> windspeed 10) (= ?loc location)]])

        session (-> (mk-session [count])
                    (insert (->WindSpeed 20 "MCI")
                            (->WindSpeed 20 "SFO")
                            (->Temperature 40 "SFO")
                            (->Temperature 50 "SFO")))]

    ;; Zero count at MCI, since no temperatures were inserted.
    (is (= {:?c 0 :?loc "MCI"} (first (query session count :?loc "MCI"))))

    ;; Ensure the two temperature readings at SFO are found as expected.
    (is (= {:?c 2 :?loc "SFO"} (first (query session count :?loc "SFO"))))))


(deftest test-accum-all
  (let [all (dsl/parse-query [] [[?t <- (acc/all) from [Temperature]]])
        all-field (dsl/parse-query [] [[?t <- (acc/all :temperature) from [Temperature]]])

        session (-> (mk-session [all all-field])
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 90 "MCI"))
                    fire-rules)

        retracted (-> session
                      (retract (->Temperature 80 "MCI"))
                      fire-rules)

        all-retracted (-> session
                          (retract (->Temperature 80 "MCI"))
                          (retract (->Temperature 80 "MCI"))
                          (retract (->Temperature 90 "MCI"))
                          fire-rules)]
    

    ;; Ensure expected items are there. We sort the query results
    ;; since ordering isn't guaranteed.
    (is (= [{:?t [(->Temperature 80 "MCI")
                  (->Temperature 80 "MCI")
                  (->Temperature 90 "MCI")]}]

           (sort-by :temperature (query session all))))

    (is (= [(->Temperature 80 "MCI")
            (->Temperature 90 "MCI")]

           (->> (query retracted all)
                (first)
                (:?t)
                (sort-by :temperature))))

    (is (= [80 80 90]
           (-> (query session all-field)
               (first)
               (:?t)
               (sort))))

    (is (= [80 90]
           (-> (query retracted all-field)
               (first)
               (:?t)
               (sort))))

    (is (= []
           (-> (query all-retracted all-field)
               first
               :?t))
        "Retracting all values should cause a return to the initial value of
        an empty sequence.")))

(deftest test-accum-all-with-bindings
  (let [mci-temps [(->Temperature 10 "MCI")
                   (->Temperature 20 "MCI")]
        sfo-temps [(->Temperature 10 "SFO")
                   (->Temperature 20 "SFO")]
        temp-rule (dsl/parse-rule [[?temps <- (acc/all) :from [Temperature (< temperature 25) (= ?location location)]]]
                                  (insert! (->TemperatureHistory (sort-by :temperature ?temps))))

        temp-history-query (dsl/parse-query [] [[TemperatureHistory (= ?temps temperatures)]])

        all-temps-session (-> (mk-session [temp-rule temp-history-query] :cache false)
                              (insert-all (concat mci-temps sfo-temps))
                              fire-rules)]


    (is (= (-> all-temps-session
               (query temp-history-query)
               frequencies)
           {{:?temps mci-temps} 1
            {:?temps sfo-temps} 1}))

    (is (= (-> (apply retract all-temps-session mci-temps)
               fire-rules
               (query temp-history-query)
               frequencies)
           {{:?temps sfo-temps} 1}))))

(deftest test-reduce-to-accum-max
  (let [max-accum (acc/reduce-to-accum
                   (fn [previous value]
                     (if previous
                       (if (> (:temperature value) (:temperature previous))
                         value
                         previous)
                       value)))

        max-query (dsl/parse-query [] [[?t <- max-accum :from [Temperature]]])

        session (-> (mk-session [max-query])
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))

        retracted (retract session (->Temperature 80 "MCI"))]

    (is (= {:?t (->Temperature 80 "MCI")} (first (query session max-query))))
    (is (= {:?t (->Temperature 30 "MCI")} (first (query retracted max-query))))))

(deftest test-reduce-to-accum-sum
  (let [sum-accum (acc/reduce-to-accum
                   (fn [previous value]
                     (+ previous (:temperature value)))
                   0
                   identity
                   +)

        sum (dsl/parse-query [] [[?t <- sum-accum :from [Temperature]]])

        session (-> (mk-session [sum])
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))

        retracted (retract session (->Temperature 30 "MCI"))]

    (is (= {:?t 120} (first (query session sum))))
    (is (= {:?t 90} (first (query retracted sum))))))

(deftest test-grouping-accum
  (let [grouping-accum (acc/grouping-by :temperature)
        grouping-query (dsl/parse-query [] [[?t <- grouping-accum
                                             :from [Temperature]]])

        convert-return-fn (fn [m]
                            (if (empty? m)
                              []
                              (val (apply max-key key m))))
        grouping-convert-accum (acc/grouping-by :temperature
                                                convert-return-fn)
        grouping-convert-query (dsl/parse-query [] [[?t <- grouping-convert-accum
                                                     :from [Temperature]]])

        session (-> (mk-session [grouping-query
                                 grouping-convert-query])
                    (insert-all [(->Temperature 30 "MCI")
                                 (->Temperature 10 "MCI")
                                 (->Temperature 80 "MCI")
                                 (->Temperature 80 "MCI")]))

        retracted-session (-> session
                              (retract (->Temperature 80 "MCI")
                                       (->Temperature 80 "MCI")))]

    (testing "grouping-accum"
      (is (= {:?t {30 [(->Temperature 30 "MCI")]
                   10 [(->Temperature 10 "MCI")]
                   80 [(->Temperature 80 "MCI")
                       (->Temperature 80 "MCI")]}}
             (first (query session grouping-query)))))

    (testing "grouping-accum with retraction"
      (is (= {:?t {30 [(->Temperature 30 "MCI")]
                   10 [(->Temperature 10 "MCI")]}}
             (first (query retracted-session grouping-query)))))

    (testing "grouping-accum with custom convert-return-fn (max temp group)"
      (is (= {:?t [(->Temperature 80 "MCI")
                   (->Temperature 80 "MCI")]}
             (first (query session grouping-convert-query)))))

    (testing "grouping-accum with custom convert-return-fn and retraction (max temp group)"
      (is (= {:?t [(->Temperature 30 "MCI")]}
             (first (query retracted-session grouping-convert-query)))))))
