(ns clara.test-accumulators
  (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
            [clara.rules :refer [fire-rules
                                 insert
                                 insert-all
                                 insert!
                                 retract
                                 query]]

            [clara.rules.testfacts :refer [->Temperature ->Cold ->WindSpeed
                                           ->TemperatureHistory]]
            [clojure.test :refer [is deftest run-tests testing use-fixtures]]
            [clara.rules.accumulators :as acc]
            [schema.test :as st])
  (:import [clara.rules.testfacts
            Temperature
            TemperatureHistory
            Cold
            WindSpeed]))

(use-fixtures :once st/validate-schemas tu/opts-fixture)

(def-rules-test test-max
  {:queries [hottest [[]
                      [[?t <- (acc/max :temperature) from [Temperature]]]]]

   :sessions [empty-session [hottest] {}]}

  (let [fired-session (-> empty-session
                          (insert (->Temperature 30 "MCI"))
                          (insert (->Temperature 10 "MCI"))
                          (insert (->Temperature 80 "MCI"))
                          fire-rules)

        retracted-session (-> fired-session
                              (insert (->Temperature 100 "MCI"))
                              (retract (->Temperature 100 "MCI"))
                              fire-rules)]

    (is (= [{:?t 80}]
           (query fired-session hottest)
           (query retracted-session hottest)))))

(def-rules-test test-min-max-retract-to-nothing
  {:rules [cold-temp-min [[[?t <- (acc/min :temperature) :from [Temperature]]]
                          (insert! (->Cold ?t))]

           cold-temp-max [[[?t <- (acc/max :temperature) :from [Temperature]]]
                          (insert! (->Cold ?t))]]

   :queries [cold-query [[]
                         [[Cold (= ?temp temperature)]]]]

   :sessions [min-session [cold-temp-min cold-query] {}
              max-session [cold-temp-max cold-query] {}]}

  (doseq [session [min-session
                   max-session]
          :let [with-cold-session (->
                                   session
                                   (insert (->Temperature 10 "MCI"))
                                   fire-rules)]]

    (is (= (query with-cold-session cold-query)
           [{:?temp 10}])
        "Without retraction there should be a Cold fact with a temperature of 10")

    (is (= (-> with-cold-session
               (retract (->Temperature 10 "MCI"))
               fire-rules
               (query cold-query))
           [])
        "When the original temperature is retracted and we go back to having no matching facts the downstream facts should be retracted.")))

(def-rules-test test-min-max-average
  {:queries [coldest [[]
                      [[?t <- (acc/min :temperature) :from [Temperature]]]]

             coldest-fact [[] [[?t <- (acc/min :temperature :returns-fact true) from [Temperature]]]]

             hottest [[] [[?t <- (acc/max :temperature) from [Temperature]]]]

             hottest-fact [[] [[?t <- (acc/max :temperature :returns-fact true) from [Temperature]]]]

             average-temp [[] [[?t <- (acc/average :temperature) from [Temperature]]]]]

   :sessions [empty-session [coldest coldest-fact hottest hottest-fact average-temp] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)

        min-retracted (-> session
                          (retract (->Temperature 10 "MCI"))
                          fire-rules)

        max-retracted (-> session
                          (retract (->Temperature 80 "MCI"))
                          fire-rules)]

    (is (empty (query empty-session coldest)))
    (is (= [{:?t 10}] (query session coldest)))
    (is (= #{{:?t (->Temperature 10 "MCI")}}
           (set (query session coldest-fact))))

    (is (empty (query empty-session hottest)))
    (is (= [{:?t 80}] (query session hottest)))

    (is (= #{{:?t (->Temperature 80 "MCI")}}
           (set (query session hottest-fact))))

    (is (empty (query empty-session average-temp)))
    (is (= (list {:?t 40}) (query session average-temp)))
    (is (= (list {:?t 20}) (query max-retracted average-temp)))))

(def-rules-test test-sum
  {:queries [sum-query [[] [[?t <- (acc/sum :temperature) from [Temperature]]]]

             sum-query-with-default [[] [[?t <- (acc/sum :temperature :default-value 10) from [Temperature]]]]]

   :sessions [empty-session [sum-query sum-query-with-default] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature nil "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)

        retracted (-> session
                      (retract (->Temperature 30 "MCI"))
                      (retract (->Temperature nil "MCI"))
                      fire-rules)]

    (is (= [{:?t 120}] (query session sum-query)))
    (is (= [{:?t 130}] (query session sum-query-with-default)))
    (is (= [{:?t 90}] (query retracted sum-query)))
    (is (= [{:?t 90}] (query retracted sum-query-with-default)))))

(def-rules-test test-count
  {:queries [count-query [[] [[?c <- (acc/count) from [Temperature]]]]]

   :sessions [empty-session [count-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)

        retracted (-> session
                      (retract (->Temperature 30 "MCI"))
                      fire-rules)]

    (is (= [{:?c 0}] (query empty-session count-query)))
    (is (= [{:?c 3}] (query session count-query)))
    (is (= [{:?c 2}] (query retracted count-query)))))

(def-rules-test test-distinct
  {:queries [distinct-query [[] [[?t <- (acc/distinct) from [Temperature]]]]
             distinct-field-query [[] [[?t <- (acc/distinct :temperature) from [Temperature]]]]]

   :sessions [empty-session [distinct-query distinct-field-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 90 "MCI"))
                    fire-rules)

        retracted (-> session
                      (retract (->Temperature 90 "MCI"))
                      fire-rules)]

    (is (= #{{:?t #{(->Temperature 80 "MCI")
                    (->Temperature 90 "MCI")}}}
           (set (query session distinct-query))))

    (is (= #{{:?t #{(->Temperature 80 "MCI")}}}
           (set (query retracted distinct-query))))

    (is (= #{{:?t #{80 90}}}
           (set (query session distinct-field-query))))

    (is (= #{{:?t #{80}}}
           (set (query retracted distinct-field-query))))

    ;; Tests for https://github.com/cerner/clara-rules/issues/325 without the field argument.
    (let [retract-one-dup (-> empty-session
                              (insert (->Temperature 80 "MCI") (->Temperature 80 "MCI"))
                              fire-rules
                              (retract (->Temperature 80 "MCI"))
                              fire-rules)

          retract-both-dups (-> retract-one-dup
                                (retract (->Temperature 80 "MCI"))
                                fire-rules)

          retract-both-dups-add-another (-> retract-one-dup
                                            (insert (->Temperature 0 "ORD"))
                                            fire-rules
                                            (retract (->Temperature 80 "MCI"))
                                            fire-rules)]

      (is (= [{:?t #{(->Temperature 80 "MCI")}}]
             (query retract-one-dup distinct-query)))

      (is (= [{:?t #{}}]
             (query retract-both-dups distinct-query)))

      (is (= [{:?t #{(->Temperature 0 "ORD")}}]
             (query retract-both-dups-add-another distinct-query))))

    ;; Tests for https://github.com/cerner/clara-rules/issues/325 with the field argument.
    (let [retract-one-dup-field (-> empty-session
                                    (insert (->Temperature 80 "MCI") (->Temperature 80 "DFW"))
                                    fire-rules
                                    (retract (->Temperature 80 "MCI"))
                                    fire-rules)

          retract-both-dups-field (-> retract-one-dup-field
                                      (retract (->Temperature 80 "DFW"))
                                      fire-rules)

          retract-both-dups-add-another-field (-> retract-one-dup-field
                                                  (insert (->Temperature 0 "MCI"))
                                                  fire-rules
                                                  (retract (->Temperature 80 "DFW"))
                                                  fire-rules)]
      (is (= [{:?t #{80}}]
             (query retract-one-dup-field distinct-field-query)))

      (is (= [{:?t #{}}]
             (query retract-both-dups-field distinct-field-query)))

      (is (= [{:?t #{0}}]
             (query retract-both-dups-add-another-field distinct-field-query))))))

(def-rules-test test-distinct-nil-field
  {:queries [distinct-windspeeds [[]
                                  [[?winds <- (acc/distinct :windspeed) :from [Temperature]]]]]

   :sessions [empty-session [distinct-windspeeds] {}]}

  (is (= [{:?winds #{nil}}]

         (-> empty-session
             (insert (->Temperature 80 "MCI"))
             fire-rules
             (query distinct-windspeeds)))))

(def-rules-test test-min-max-average-multi-condition-join

  {:queries [max-min-avg [[] [[?max <- (acc/max :temperature) from [Temperature]]
                              [?min <- (acc/min :temperature) from [Temperature]]
                              [?avg <- (acc/average :temperature) from [Temperature]]]]]

   :sessions [empty-session [max-min-avg] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 30 "MCI")
                            (->Temperature 10 "MCI")
                            (->Temperature 80 "MCI"))
                    fire-rules)]

    (is (= [{:?max 80 :?min 10 :?avg 40}] (query session max-min-avg)))))

(def-rules-test test-count-none-joined

  {:queries [count-query-acc-last [[] [[WindSpeed (> windspeed 10) (= ?loc location)]
                                       [?c <- (acc/count) from [Temperature (= ?loc location)]]]]

             ;; Putting the accumulator first validates that reordering of the
             ;; conditions in the compiler works.
             count-query-acc-first [[]
                                    [[?c <- (acc/count) from [Temperature (= ?loc location)]]
                                     [WindSpeed (> windspeed 10) (= ?loc location)]]]]

   :sessions [empty-session-acc-first [count-query-acc-first] {}
              empty-session-acc-last [count-query-acc-last] {}]}

  (doseq [[empty-session count-query desc] [[empty-session-acc-first count-query-acc-first "Accumulator first"]
                                            [empty-session-acc-last count-query-acc-last "Accumulator last"]]]
    (is (= [{:?c 0 :?loc "MCI"}]
           (-> empty-session
               (insert (->WindSpeed 20 "MCI"))
               fire-rules
               (query count-query)))
        desc)))

(def-rules-test test-count-some-empty
  {:queries [count-query [[:?loc] [[?c <- (acc/count) from [Temperature (= ?loc location)]]
                                   [WindSpeed (> windspeed 10) (= ?loc location)]]]]

   :sessions [empty-session [count-query] {}]}

  (let [session (-> empty-session
                    (insert (->WindSpeed 20 "MCI")
                            (->WindSpeed 20 "SFO")
                            (->Temperature 40 "SFO")
                            (->Temperature 50 "SFO"))
                    fire-rules)]

    ;; Zero count at MCI, since no temperatures were inserted.
    (is (= [{:?c 0 :?loc "MCI"}] (query session count-query :?loc "MCI")))

    ;; Ensure the two temperature readings at SFO are found as expected.
    (is (= [{:?c 2 :?loc "SFO"}] (query session count-query :?loc "SFO")))))

(def-rules-test test-accum-all
  {:queries [all [[] [[?t <- (acc/all) from [Temperature]]]]
             all-field [[]
                        [[?t <- (acc/all :temperature) from [Temperature]]]]]

   :sessions [empty-session [all all-field] {}]}

  (let [session (-> empty-session
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

(def-rules-test test-accum-all-with-bindings
  {:rules [temp-rule [[[?temps <- (acc/all) :from [Temperature (< temperature 25) (= ?location location)]]]
                      (insert! (->TemperatureHistory (sort-by :temperature ?temps)))]]

   :queries [temp-history-query [[]
                                 [[TemperatureHistory (= ?temps temperatures)]]]]

   :sessions [empty-session [temp-rule temp-history-query] {}]}

  (let [mci-temps [(->Temperature 10 "MCI")
                   (->Temperature 20 "MCI")]
        sfo-temps [(->Temperature 10 "SFO")
                   (->Temperature 20 "SFO")]

        all-temps-session (-> empty-session
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

(def reduced-max-accum
  (acc/reduce-to-accum
   (fn [previous value]
     (if previous
       (if (> (:temperature value) (:temperature previous))
         value
         previous)
       value))))

(def-rules-test test-reduce-to-accum-max

  {:queries [temp-query [[] [[?t <- reduced-max-accum :from [Temperature]]]]]

   :sessions [empty-session [temp-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)

        retracted (-> session
                      (retract (->Temperature 80 "MCI"))
                      fire-rules)]

    (is (= [{:?t (->Temperature 80 "MCI")}] (query session temp-query)))
    (is (= [{:?t (->Temperature 30 "MCI")}] (query retracted temp-query)))))

(def reduced-sum-accum
  (acc/reduce-to-accum
   (fn [previous value]
     (+ previous (:temperature value)))
   0
   identity
   +))

(def-rules-test test-reduce-to-accum-sum

  {:queries [sum [[]
                  [[?t <- reduced-sum-accum :from [Temperature]]]]]

   :sessions [empty-session [sum] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)

        retracted (-> session
                      (retract (->Temperature 30 "MCI"))
                      fire-rules)]

    (is (= [{:?t 120}] (query session sum)))
    (is (= [{:?t 90}] (query retracted sum)))))

(def-rules-test test-grouping-accum

  {:queries [grouping-query [[] [[?t <- (acc/grouping-by :temperature)
                                  :from [Temperature]]]]

             grouping-convert-query [[] [[?t <- (acc/grouping-by
                                                 :temperature
                                                 (fn [m]
                                                   (if (empty? m)
                                                     []
                                                     (val (apply max-key key m)))))
                                          :from [Temperature]]]]]

   :sessions [empty-session [grouping-query grouping-convert-query] {}]}

  (let [session (-> empty-session
                    (insert-all [(->Temperature 30 "MCI")
                                 (->Temperature 10 "MCI")
                                 (->Temperature 80 "MCI")
                                 (->Temperature 80 "MCI")])
                    fire-rules)

        retracted-session (-> session
                              (retract (->Temperature 80 "MCI")
                                       (->Temperature 80 "MCI"))
                              fire-rules)]

    (testing "grouping-accum"
      (is (= [{:?t {30 [(->Temperature 30 "MCI")]
                    10 [(->Temperature 10 "MCI")]
                    80 [(->Temperature 80 "MCI")
                        (->Temperature 80 "MCI")]}}]
             (query session grouping-query))))

    (testing "grouping-accum with retraction"
      (is (= [{:?t {30 [(->Temperature 30 "MCI")]
                    10 [(->Temperature 10 "MCI")]}}]
             (query retracted-session grouping-query))))

    (testing "grouping-accum with custom convert-return-fn (max temp group)"
      (is [{:?t [(->Temperature 80 "MCI")
                 (->Temperature 80 "MCI")]}]
          (query session grouping-convert-query)))

    (testing "grouping-accum with custom convert-return-fn and retraction (max temp group)"
      (is (= [{:?t [(->Temperature 30 "MCI")]}]
             (query retracted-session grouping-convert-query))))))

;; Validate that the inital vector will remain as a vector even after a retraction occurs
;; See https://github.com/cerner/clara-rules/issues/338
(def-rules-test test-data-structure-consistency-post-retraction
  {:queries [cold-query [[] [[?all-cold <- (acc/all) :from [Cold]]]]

             wind-query [[] [[?all-wind <- (assoc (acc/all) :initial-value '()) :from [WindSpeed]]]]]

   :sessions [empty-session [cold-query wind-query] {}]}

  (let [session (-> empty-session
                    (insert-all [(->Cold -1)
                                 (->Cold 0)
                                 (->WindSpeed 10 "MCI")
                                 (->WindSpeed 75 "KCI")])
                    fire-rules
                    (retract (->Cold -1) (->WindSpeed 10 "MCI"))
                    fire-rules)

        cold-facts (-> (query session cold-query)
                       first
                       :?all-cold)

        wind-facts (-> (query session wind-query)
                       first
                       :?all-wind)]

    (testing "Default initial value structure is maintained"
      (is (and (= (count (query session cold-query)) 1)
               (= [(->Cold 0)] cold-facts)
               (vector? cold-facts))))

    (testing "Custom initial value structure is maintained"
      (is (and (= (count (query session wind-query)) 1)
               (= [(->WindSpeed 75 "KCI")] wind-facts)
               (seq? wind-facts))))))
