#?(:clj
   (ns clara.test-accumulation
     (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert-all
                                    insert-all!
                                    insert!
                                    retract
                                    query
                                    accumulate]]
               [clara.rules.accumulators :as acc]
               [clara.rules.testfacts :refer [->Temperature ->Cold ->WindSpeed
                                              ->ColdAndWindy map->FlexibleFields ->TemperatureHistory
                                              ->First ->Second ->Hot ->LousyWeather]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature
               Cold
               WindSpeed
               ColdAndWindy
               FlexibleFields
               TemperatureHistory
               First
               Second
               Hot
               LousyWeather]))

   :cljs
   (ns clara.test-accumulation
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    insert-all
                                    insert-all!
                                    retract
                                    query
                                    accumulate]]
               [clara.rules.testfacts :refer [map->FlexibleFields FlexibleFields
                                              ->Temperature Temperature
                                              ->Cold Cold
                                              ->WindSpeed WindSpeed
                                              ->ColdAndWindy ColdAndWindy
                                              ->TemperatureHistory TemperatureHistory
                                              ->First First
                                              ->Second Second
                                              ->Hot Hot
                                              ->LousyWeather LousyWeather]]
               [clara.rules.accumulators :as acc]
               [clara.tools.testing-utils :as tu]
               [cljs.test]
               [schema.test :as st])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

;; Tests focused on DSL functionality such as binding visibility in edge cases, fact field access, etc.
;; The distinction between tests here and tests in files focused on the aspects of Clara that the DSL represents
;; may not be clear and there are borderline cases.

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))

(def side-effect-holder (atom nil))

;; In ClojureScript the type function does not use the :type key in metadata as in Clojure, but we had
;; tests in Clojure that were relying on this.  Creating a custom function for the fact type along these
;; lines was the easiest way to get the tests relying on this behavior to pass in ClojureScript.  I used
;; data on the actual fact rather than metadata and updated the tests accordingly.  This is desirable since
;; Clara's value-based semantics won't consider the metadata part of the fact for the purpose of memory operations.
(defn type-or-class
  [fact]
  (or (:type fact)
      (type fact)))

(def-rules-test test-simple-binding-variable-ordering

  {:rules [rule1 [[[Temperature (< temperature 20) (= ?t temperature)]]
                  (reset! side-effect-holder ?t)]

           rule2 [[[Temperature (= ?t temperature) (< temperature 20)]]
                  (reset! side-effect-holder ?t)]

           rule3 [[[Temperature (< temperature 20) (= temperature ?t)]]
                  (reset! side-effect-holder ?t)]

           rule4 [[[Temperature (= temperature ?t) (< temperature 20)]]
                  (reset! side-effect-holder ?t)]]

   :sessions [rule1-session [rule1] {}
              rule2-session [rule2] {}
              rule3-session [rule3] {}
              rule4-session [rule4] {}]}

  (testing "Unrelated constraint on the field and then a binding with the variable first"
    (reset! side-effect-holder nil)
    
    (-> rule1-session
        (insert (->Temperature 10 "MCI"))
        fire-rules)

    (is (= 10 @side-effect-holder))))

(defn identity-retract
  "Retract function that does nothing for testing purposes."
  [state retracted]
  state)

(defn min-fact
  "Function to create a new accumulator for a test."
  [field]
  (accumulate
   :retract-fn identity-retract
   :reduce-fn (fn [value item]
                (if (or (= value nil)
                        (< (field item) (field value) ))
                  item
                  value))))

(def-rules-test test-simple-accumulator
  {:queries [coldest-query [[] [[?t <- (accumulate
                                        :retract-fn identity-retract
                                        :reduce-fn (fn [value item]
                                                     (if (or (= value nil)
                                                             (< (:temperature item) (:temperature value) ))
                                                       item
                                                       value)))
                                 from [Temperature]]]]

             coldest-query-defined [[] [[?t <- (min-fact :temperature) from [Temperature]]]]]

   :sessions [empty-session [coldest-query coldest-query-defined] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)]

    ;; Accumulator returns the lowest value.
    (is (= #{{:?t (->Temperature 10 "MCI")}}
           (set (query session coldest-query-defined))
           (set (query session coldest-query))))))

(def-rules-test test-query-accumulator-output

  {:rules [set-cold [[[?t <- (min-fact :temperature) from [Temperature]]]
                     (insert! (->Cold (:temperature ?t)))]]

   :queries [cold-query [[] [[?c <- Cold]]]]

   :sessions [empty-session [set-cold cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    (fire-rules))]

    ;; Accumulator returns the lowest value.
    (is (= #{{:?c (->Cold 10)}}
           (set (query session cold-query))))))

(defn average-value
  "Test accumulator that returns the average of a field"
  [field]
  (accumulate
   :initial-value [0 0]
   :reduce-fn (fn [[value count] item]
                [(+ value (field item)) (inc count)])
   :combine-fn (fn [[value1 count1] [value2 count2]]
                 [(+ value1 value2) (+ count1 count2)])
   :retract-fn identity-retract
   :convert-return-fn (fn [[value count]] (if (= 0 count)
                                            nil
                                            (/ value count)))))

(def-rules-test test-accumulator-with-result

  {:queries [average-temp-query [[] [[?t <- (average-value :temperature) from [Temperature]]]]]

   :sessions [empty-session [average-temp-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 20 "MCI"))
                    (insert (->Temperature 30 "MCI"))
                    (insert (->Temperature 40 "MCI"))
                    (insert (->Temperature 50 "MCI"))
                    (insert (->Temperature 60 "MCI"))
                    fire-rules)]

    ;; Accumulator returns the lowest value.
    (is (= #{{:?t 35}}
           (set (query session average-temp-query))))))

(def-rules-test test-accumulate-with-retract

  {:queries [coldest-query [[] [[?t <- (accumulate
                                        :initial-value []
                                        :reduce-fn conj
                                        :combine-fn concat

                                        ;; Retract by removing the retracted item.
                                        ;; In general, this would need to remove
                                        ;; only the first matching item to achieve expected semantics.
                                        :retract-fn (fn [reduced item]
                                                      (remove #{item} reduced))

                                        ;; Sort here and return the smallest.
                                        :convert-return-fn (fn [reduced]
                                                             (first
                                                              (sort #(< (:temperature %1) (:temperature %2))
                                                                    reduced))))

                                 :from (Temperature (< temperature 20))]]]]

   :sessions [empty-session [coldest-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 17 "MCI"))
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    (retract (->Temperature 10 "MCI"))
                    fire-rules)]

    ;; The accumulator result should be
    (is (= #{{:?t (->Temperature 15 "MCI")}}
           (set (query session coldest-query))))))

(def-rules-test test-joined-accumulator

  {:queries [coldest-query-simple-join [[]
                                        [(WindSpeed (= ?loc location))
                                         [?t <- (accumulate
                                                 :retract-fn identity-retract
                                                 :reduce-fn (fn [value item]
                                                              (if (or (= value nil)
                                                                      (< (:temperature item) (:temperature value) ))
                                                                item
                                                                value)))
                                          :from (Temperature (= ?loc location))]]]

             coldest-query-complex-join [[]
                                         [(WindSpeed (= ?loc location))
                                          [?t <- (accumulate
                                                  :retract-fn identity-retract
                                                  :reduce-fn (fn [value item]
                                                               (if (or (= value nil)
                                                                       (< (:temperature item) (:temperature value) ))
                                                                 item
                                                                 value)))
                                           :from (Temperature (tu/join-filter-equals ?loc location))]]]]

   :sessions [simple-join-session [coldest-query-simple-join] {}
              complex-join-session [coldest-query-complex-join] {}]}

  (doseq [[empty-session
           coldest-query
           node-type]

          [[simple-join-session coldest-query-simple-join "AccumulateNode"]
           [complex-join-session coldest-query-complex-join "AccumulateWithJoinFilterNode"]]

          :let [session (-> empty-session
                            (insert (->Temperature 15 "MCI"))
                            (insert (->Temperature 10 "MCI"))
                            (insert (->Temperature 5 "SFO"))

                            ;; Insert last to exercise left activation of accumulate node.
                            (insert (->WindSpeed 30 "MCI"))
                            fire-rules)

                session-retracted (-> session
                                      (retract (->WindSpeed 30 "MCI"))
                                      fire-rules)

                session-other-retracted (-> session
                                            (insert (->WindSpeed 30 "ORD"))
                                            (retract (->WindSpeed 30 "ORD"))
                                            fire-rules)]]

    ;; Only the value that joined to WindSpeed should be visible.
    (is (= #{{:?t (->Temperature 10 "MCI") :?loc "MCI"}}
           (set (query session coldest-query)))
        (str "Simple one-pass selection of the minimum for node type " node-type))

    (is (= #{{:?t (->Temperature 10 "MCI") :?loc "MCI"}}
           (-> session-other-retracted (query coldest-query) set))
        (str "Adding and retracting a Location with another location should have no impact on the previous joined facts for node type " node-type))

    (is (empty? (query session-retracted coldest-query))
        (str "Retracting the WindSpeed fact that the temperatures joined with should cause the results from the query to be retracted for node type " node-type))))

(def-rules-test test-bound-accumulator-var

  {:queries [coldest-query [[:?loc]
                            [[?t <- (accumulate
                                     :retract-fn identity-retract
                                     :reduce-fn (fn [value item]
                                                  (if (or (= value nil)
                                                          (< (:temperature item) (:temperature value) ))
                                                    item
                                                    value)))
                              :from [Temperature (= ?loc location)]]]]]

   :sessions [empty-session [coldest-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 5 "SFO"))
                    fire-rules)]

    (is (= #{{:?t (->Temperature 10 "MCI") :?loc "MCI"}}
           (set (query session coldest-query :?loc "MCI"))))

    (is (= #{{:?t (->Temperature 5 "SFO") :?loc "SFO"}}
           (set (query session coldest-query :?loc "SFO"))))))

(def-rules-test test-accumulator-rule-with-no-fact-binding

  {:rules [rule [[[(accumulate :initial-value []
                                           :reduce-fn conj
                                           :retract-fn identity-retract
                                           :combine-fn into)
                               from [WindSpeed]]]
                 (reset! side-effect-holder true)]]

   :sessions [empty-session [rule] {}]}

  (reset! side-effect-holder nil)
  
  (-> empty-session
      (insert (->WindSpeed 20 "MCI")
              (->WindSpeed 20 "SFO"))
      (fire-rules))

  (is (true? @side-effect-holder)))

(def-rules-test test-accumulator-with-test-join-single-type

  {:queries [colder-than-mci-query [[]
                                    [[Temperature (= "MCI" location) (= ?mci-temp temperature)]
                                     [?colder-temps <- (acc/all)
                                      :from [Temperature (< temperature ?mci-temp)]]]]]

   :sessions [empty-session [colder-than-mci-query] {}]}

  (let [check-result (fn [session]
                       (let [results (query session colder-than-mci-query)]

                         (is (= 1 (count results)))

                         (is (= {:?mci-temp 15
                                 :?colder-temps #{(->Temperature 10 "ORD")
                                                  (->Temperature 5 "LGA")}}

                                ;; Convert temps to a set, since ordering isn't guaranteed.
                                (update-in
                                 (first results)
                                 [:?colder-temps]
                                 set)))))]

    
    ;; Simple insertion of facts at once.
    (check-result (-> empty-session
                      (insert (->Temperature 15 "MCI")
                              (->Temperature 10 "ORD")
                              (->Temperature 5 "LGA")
                              (->Temperature 30 "SFO"))
                      (fire-rules)))


    ;; Insert facts separately to ensure they are combined.
    (check-result (-> empty-session
                      (insert (->Temperature 15 "MCI")
                              (->Temperature 5 "LGA")
                              (->Temperature 30 "SFO"))
                      (insert (->Temperature 10 "ORD"))
                      (fire-rules)))

    ;; Insert MCI location last to test left activation with token arriving
    ;; after the initial accumulation
    (check-result (-> empty-session
                      (insert (->Temperature 10 "ORD")
                              (->Temperature 5 "LGA")
                              (->Temperature 30 "SFO"))
                      (insert (->Temperature 15 "MCI"))
                      (fire-rules)))

    ;; Test retraction of previously accumulated fact.
    (check-result (-> empty-session
                      (insert (->Temperature 15 "MCI")
                              (->Temperature 10 "ORD")
                              (->Temperature 5 "LGA")
                              (->Temperature 30 "SFO")
                              (->Temperature 0 "IAD"))
                      (retract (->Temperature 0 "IAD"))
                      (fire-rules)))

    ;; Test retraction and re-insertion of token.
    (check-result (-> empty-session
                      (insert (->Temperature 10 "ORD")
                              (->Temperature 5 "LGA")
                              (->Temperature 30 "SFO"))
                      (insert (->Temperature 15 "MCI"))
                      (retract (->Temperature 15 "MCI"))
                      (insert (->Temperature 15 "MCI"))
                      (fire-rules)))))

;; Testing that a join filter accumulate node with no initial value will
;; only propagate results when candidate facts pass the join filter.
(def-rules-test test-accumulator-with-test-join-multi-type

  {:queries [get-cold-temp [[] [[?cold <- Cold]]]]

   :rules [get-min-temp-under-threshhold [[[?threshold <- :temp-threshold]

                                           [?min-temp <- (acc/min :temperature)
                                            :from
                                            [Temperature
                                             (< temperature (:temperature ?threshold))]]]
                                          (insert! (->Cold ?min-temp))]]

   :sessions [session [get-cold-temp get-min-temp-under-threshhold] {:fact-type-fn type-or-class}
              simple-session [get-min-temp-under-threshhold] {:fact-type-fn type-or-class}]}

  (let [;; Test assertion helper.
        assert-query-results (fn [test-name session & expected-results]

                               (is (= (count expected-results)
                                      (count (query session get-cold-temp)))
                                   (str test-name (seq (query session get-cold-temp))))

                               (is (= (set expected-results)
                                      (set (query session get-cold-temp)))
                                   (str test-name)))

        thresh-10 {:temperature 10
                   :type :temp-threshold}

        thresh-20 {:temperature 20
                   :type :temp-threshold}

        temp-5-mci (->Temperature 5 "MCI")
        temp-10-lax (->Temperature 10 "LAX")
        temp-20-mci (->Temperature 20 "MCI")]

    ;; No temp tests - no firing

    (assert-query-results 'no-thresh-no-temps
                          session)

    (assert-query-results 'one-thresh-no-temps
                          (-> session
                              (insert thresh-10)
                              fire-rules))

    (assert-query-results 'retract-thresh-no-temps
                          (-> session
                              (insert thresh-10)
                              fire-rules
                              (retract thresh-10)
                              fire-rules))

    (assert-query-results 'two-thresh-no-temps
                          (-> session
                              (insert thresh-10)
                              (insert thresh-20)
                              fire-rules))

    ;; With temps tests.

    (assert-query-results 'one-thresh-one-temp-no-match
                          (-> session
                              (insert thresh-10)
                              (insert temp-10-lax)
                              fire-rules))

    (assert-query-results 'one-thresh-one-temp-no-match-retracted
                          (-> session
                              (insert thresh-10)
                              (insert temp-10-lax)
                              fire-rules
                              (retract temp-10-lax)
                              fire-rules))

    (assert-query-results 'two-thresh-one-temp-no-match
                          (-> session
                              (insert thresh-10)
                              (insert thresh-20)
                              (insert temp-20-mci)
                              fire-rules))

    (assert-query-results 'two-thresh-one-temp-two-match
                          (-> session
                              (insert thresh-10)
                              (insert thresh-20)
                              (insert temp-5-mci)
                              fire-rules)

                          {:?cold (->Cold 5)}
                          {:?cold (->Cold 5)})

    (assert-query-results 'one-thresh-one-temp-one-match
                          (-> session
                              (insert thresh-20)
                              (insert temp-5-mci)
                              fire-rules)

                          {:?cold (->Cold 5)})

    (assert-query-results 'retract-thresh-one-temp-one-match
                          (-> session
                              (insert thresh-20)
                              (insert temp-5-mci)
                              fire-rules
                              (retract thresh-20)
                              fire-rules))

    (assert-query-results 'one-thresh-two-temp-one-match
                          (-> session
                              (insert thresh-20)
                              (insert temp-5-mci)
                              (insert temp-20-mci)
                              fire-rules)

                          {:?cold (->Cold 5)})

    (assert-query-results 'one-thresh-one-temp-one-match-retracted
                          (-> session
                              (insert thresh-20)
                              (insert temp-5-mci)
                              fire-rules
                              (retract temp-5-mci)
                              fire-rules))

    (assert-query-results 'one-thresh-two-temp-two-match
                          (-> session
                              (insert thresh-20)
                              (insert temp-5-mci)
                              (insert temp-10-lax)
                              fire-rules)

                          {:?cold (->Cold 5)})))

   ;; A test to make sure the appropriate data is held in the memory
   ;; of accumulate node with join filter is correct upon right-retract.
(def-rules-test test-accumulator-with-test-join-retract-accumulated-use-new-result

  {:rules [coldest-temp [[[?thresh <- :temp-threshold]
                          [?temp <- (acc/max :temperature)
                           :from [Temperature (< temperature (:temperature ?thresh))]]]

                         (insert! (->Cold ?temp))]]

   :queries [find-cold [[] [[?c <- Cold]]]]

   :sessions [empty-session [coldest-temp find-cold] {:fact-type-fn type-or-class}]}

  (let [thresh-20 {:temperature 20
                   :type :temp-threshold}

        temp-10-mci (->Temperature 10 "MCI")
        temp-15-lax (->Temperature 15 "LAX")

        cold-results (-> empty-session
                         (insert thresh-20
                                 temp-10-mci)
                         ;; Retract it and add it back so that an
                         ;; accumulate happens on the intermediate
                         ;; node memory state.
                         (retract temp-10-mci)
                         (insert temp-10-mci)
                         fire-rules
                         (query find-cold))]

    (is (= [{:?c (->Cold 10)}]
           cold-results))))

(def-rules-test test-accumulator-right-retract-before-matching-tokens-exist

  {:queries [accum-q [[]
                      [[Temperature (= ?t temperature)]
                       ;; Using simple AccumulateNode
                       [?cs <- (acc/all) :from [Cold (= ?t temperature)]]]]

             accum-join-filter-q [[]
                                  [[Temperature (= ?t temperature)]
                                   ;; Using AccumulateWithJoinFilterNode
                                   [?cs <- (acc/all) :from [Cold (<= temperature ?t)]]]]]

   :sessions [s1 [accum-q] {}
              s2 [accum-join-filter-q] {}]}

  (let [c10 (->Cold 10)
        t10 (->Temperature 10 "MCI")

        validate-query (fn [s q msg]
                         (is (= #{{:?t 10 :?cs []}}
                                (-> s
                                    (insert c10)
                                    (retract c10)
                                    (insert t10)
                                    fire-rules
                                    (query q)
                                    set))
                             msg))]

    (validate-query s1
                    accum-q
                    "AccumulateNode")
    (validate-query s2
                    accum-join-filter-q
                    "AccumulateWithJoinFilterNode")))

(def-rules-test test-retract-fact-never-inserted-from-accum

  {:queries [temp-sum-no-join [[] [[?s <- (acc/sum :temperature) :from [Temperature (= ?t temperature)]]]]

             temp-sum-complex-join [[] [[Cold (= ?t temperature)]
                                        [?s <- (acc/sum :temperature) :from [Temperature (< ?t temperature)]]]]]

   :sessions [empty-session [temp-sum-no-join] {}
              empty-session-complex-join [temp-sum-complex-join] {}]}

  (let [res (-> empty-session
                (insert (->Temperature 10 "LAX"))
                (retract (->Temperature 10 "MCI")) ; retracted under same bindings as inserted, but never actually inserted
                fire-rules
                (query temp-sum-no-join))]
    
    (is (= (frequencies [{:?s 10 :?t 10}])
           (frequencies res))))

  (let [res (-> empty-session-complex-join
                (insert (->Cold 9)
                        (->Temperature 10 "LAX"))
                (retract (->Temperature 10 "MCI")) ; retracted, but never inserted in the first place
                fire-rules
                (query temp-sum-complex-join))]

    (is (= (frequencies [{:?s 10 :?t 9}])
           (frequencies res)))))

(def-rules-test test-accumulator-with-init-and-binding-groups

  {:queries [get-temp-history [[] [[?his <- TemperatureHistory]]]]

   :rules [get-temps-under-threshhold [[[?threshold <- :temp-threshold]

                                        [?temps <- (acc/all) :from [Temperature (= ?loc location)
                                                                    (< temperature (:temperature ?threshold))]]]

                                       (insert! (->TemperatureHistory ?temps))]]

   :sessions [session [get-temp-history get-temps-under-threshhold] {:fact-type-fn type-or-class}]}

  (let [thresh-11 {:temperature 11
                   :type :temp-threshold}
        thresh-20  {:temperature 20
                    :type :temp-threshold}

        temp-10-mci (->Temperature 10 "MCI")
        temp-15-lax (->Temperature 15 "LAX")
        temp-20-mci (->Temperature 20 "MCI")

        two-groups-one-init (-> session
                                (insert thresh-11
                                        temp-10-mci
                                        temp-15-lax
                                        temp-20-mci)
                                fire-rules
                                (query get-temp-history))

        two-groups-no-init (-> session
                               (insert thresh-20
                                       temp-10-mci
                                       temp-15-lax
                                       temp-20-mci)
                               fire-rules
                               (query get-temp-history))]

    (is (empty?
         (-> session
             (insert thresh-11)
             fire-rules
             (query get-temp-history))))

    (is (= (frequencies [{:?his (->TemperatureHistory [temp-10-mci])}])
           (frequencies two-groups-one-init)))

    (is (= 2 (count two-groups-no-init)))
    (is (= #{{:?his (->TemperatureHistory [temp-15-lax])}
             {:?his (->TemperatureHistory [temp-10-mci])}}

           (set two-groups-no-init)))))

(def-rules-test test-multi-accumulators-together-with-initial-value

  {:rules [r [[[?f <- (acc/all) :from [First]]
               [?s <- (acc/all) :from [Second]]]
              (insert! {:f ?f
                        :s ?s
                        :type :test})]]

   :queries [q [[]
                [[?test <- :test]]]]

   :sessions [s [r q] {:fact-type-fn type-or-class}]}

  (let [batch-inserts (-> s
                          (insert (->First) (->Second))
                          fire-rules
                          (query q))

        single-inserts (-> s
                           (insert (->First))
                           (insert (->Second))
                           fire-rules
                           (query q))]

    (is (= 1
           (count batch-inserts)
           (count single-inserts)))

    (is (= #{{:?test {:f [(->First)]
                      :s [(->Second)]
                      :type :test}}}
           (set batch-inserts)
           (set single-inserts)))))

(def-rules-test test-accum-needing-token-partitions-correctly-on-fact-binding

  {:queries [qhash [[]
                    [[WindSpeed (= ?ws windspeed)]
                     [?ts <- (acc/all) :from [Temperature (= ?loc location)]]]]

             qfilter [[]
                      [[WindSpeed (= ?ws windspeed)]
                       [?ts <- (acc/all) :from [Temperature (= ?loc location)
                                                (not (tu/join-filter-equals temperature ?ws))]]]]]

   :sessions [qhash-session [qhash] {}
              qfilter-session [qfilter] {}]}

  (doseq [:let [ws10mci (->WindSpeed 10 "MCI")
                t1mci (->Temperature 1 "MCI")
                t2lax (->Temperature 2 "LAX")]
          [s q join-type] [[qhash-session qhash "hash join"]
                           [qfilter-session qfilter "filter join"]]

          :let [right-activates-before-left (-> s
                                                (insert t1mci
                                                        t2lax)
                                                (insert ws10mci)
                                                fire-rules)
                left-activates-before-right (-> s
                                                (insert ws10mci)
                                                (insert t1mci
                                                        t2lax)
                                                fire-rules)
                batched-inserts (-> s
                                    (insert ws10mci
                                            t1mci
                                            t2lax)
                                    fire-rules)
                qresults (fn [s]
                           (frequencies (query s q)))]]

    (testing (str "Testing for inserts for " join-type)
      (is (= (-> [{:?ws 10 :?ts [t1mci] :?loc "MCI"}
                  {:?ws 10 :?ts [t2lax] :?loc "LAX"}]
                 frequencies)
             (-> right-activates-before-left qresults)
             (-> left-activates-before-right qresults)
             (-> batched-inserts qresults))))

    (testing (str "Testing for retracts for " join-type)
      (is (every? empty?
                  [(-> right-activates-before-left (retract ws10mci) fire-rules qresults)
                   (-> left-activates-before-right (retract ws10mci) fire-rules qresults)
                   (-> batched-inserts (retract ws10mci) fire-rules qresults)])))))

(def-rules-test test-retract-initial-value

  {:queries [get-temp-history [[]
                               [[?his <- TemperatureHistory]]]]

   :rules [get-temps-under-threshold [[[?temps <- (acc/all) :from [Temperature (= ?loc location)]]]
                                      (insert! (->TemperatureHistory ?temps))]]

   :sessions [empty-session [get-temp-history get-temps-under-threshold] {}]}

  (let [temp-10-mci (->Temperature 10 "MCI")

        temp-history (-> empty-session
                         (insert temp-10-mci)
                         (fire-rules)
                         (query get-temp-history))

        empty-history (-> empty-session
                          (fire-rules)
                          (query get-temp-history))]

    (is (empty? empty-history)
        "An accumulate condition that creates new bindings should not propagate even with an initial value.")

    (is (= 1 (count temp-history)))
    (is (= [{:?his (->TemperatureHistory [temp-10-mci])}]
           temp-history))))


(def-rules-test test-retract-initial-value-filtered
  {:queries [get-temp-history [[] [[?his <- TemperatureHistory]]]]

   :rules [get-temps-under-threshold [[[?threshold <- :temp-threshold]

                                       [?temps <- (acc/all) :from [Temperature (= ?loc location)
                                                                   (< temperature (:temperature ?threshold))]]]

                                      (insert! (->TemperatureHistory ?temps))]]

   :sessions [empty-session [get-temp-history get-temps-under-threshold] {:fact-type-fn type-or-class}]}

  (let [thresh-11 {:temperature 11
                   :type :temp-threshold}

        temp-10-mci (->Temperature 10 "MCI")
        temp-15-lax (->Temperature 15 "LAX")
        temp-20-mci (->Temperature 20 "MCI")

        temp-history (-> empty-session
                         (insert thresh-11) ;; Explicitly insert this first to expose condition.
                         (insert temp-10-mci
                                 temp-15-lax
                                 temp-20-mci)

                         (fire-rules)
                         (query get-temp-history))

        empty-history (-> empty-session
                          (insert thresh-11)
                          (fire-rules)
                          (query get-temp-history))]

    (is (empty? empty-history))

    (is (= (frequencies [{:?his (->TemperatureHistory [temp-10-mci])}])
           (frequencies temp-history)))))

(def-rules-test test-join-to-result-binding

  {:queries [same-wind-and-temp [[]
                                 [[?t <- (acc/min :temperature) :from [Temperature]]
                                  [?w <- WindSpeed (= ?t windspeed)]]]]

   :sessions [session [same-wind-and-temp] {}]}

  (is (empty?
       (-> session
           (insert (->WindSpeed 50 "MCI")
                   (->Temperature 51 "MCI"))
           fire-rules
           (query same-wind-and-temp))))

  (is (= [{:?w (->WindSpeed 50 "MCI") :?t 50}]
         (-> session
             (insert (->WindSpeed 50 "MCI")
                     (->Temperature 50 "MCI"))
             fire-rules
             (query same-wind-and-temp)))))

(def maybe-nil-min-temp (accumulate
                         :retract-fn identity-retract
                         :reduce-fn (fn [value item]
                                      (let [t (:temperature item)]
                                        ;; When no :temperature return `value`.
                                        ;; Note: `value` could be nil.
                                        (if (and t
                                                 (or (= value nil)
                                                     (< t (:temperature value))))
                                          item
                                          value)))))

(def-rules-test test-nil-accum-reduced-has-tokens-retracted-when-new-item-inserted
  
  {:rules [coldest-temp-rule-no-join [[[?coldest <- maybe-nil-min-temp :from [Temperature]]]

                                      (insert! (->Cold (:temperature ?coldest)))]

           coldest-temp-rule-join [[[:max-threshold [{:keys [temperature]}]
                                     (= ?max-temp temperature)]

                                    ;; Note a non-equality based unification.
                                    ;; Gets max temp under a given max threshold.
                                    [?coldest <- maybe-nil-min-temp :from [Temperature
                                                                           ;; Gracefully handle nil.
                                                                           (< (or temperature 0)
                                                                              ?max-temp)]]]

                                   (insert! (->Cold (:temperature ?coldest)))]]

   :queries [coldest-temp-query [[] [[?cold <- Cold]]]]

   :sessions [session-no-join [coldest-temp-rule-no-join coldest-temp-query] {:fact-type-fn type-or-class}
              session-join [coldest-temp-rule-join coldest-temp-query] {:fact-type-fn type-or-class}]}

  (let [temp-nil (->Temperature nil "MCI")
        temp-10 (->Temperature 10 "MCI")

        insert-nil-first-session-no-join (-> session-no-join
                                             (insert temp-nil)
                                             (insert temp-10)
                                             fire-rules)
        insert-nil-second-session-no-join (-> session-no-join
                                              (insert temp-10)
                                              (insert temp-nil)
                                              fire-rules)

        insert-nil-first-session-join (-> session-join
                                          (insert {:temperature 15
                                                   :type :max-threshold})
                                          (insert temp-nil)
                                          (insert temp-10)
                                          fire-rules)
        insert-nil-second-session-join (-> session-join
                                           (insert {:temperature 15
                                                    :type :max-threshold})
                                           (insert temp-10)
                                           (insert temp-nil)
                                           fire-rules)]

    (is (= (count (query insert-nil-first-session-no-join coldest-temp-query))
           (count (query insert-nil-second-session-no-join coldest-temp-query)))
        "Failed expected counts when flipping insertion order for AccumulateNode.")

    (is (= #{{:?cold (->Cold 10)}}
           (set (query insert-nil-first-session-no-join coldest-temp-query))
           (set (query insert-nil-second-session-no-join coldest-temp-query)))
        "Failed expected query results when flipping insertion order for AccumulateNode.")

    (is (= (count (query insert-nil-first-session-join coldest-temp-query))
           (count (query insert-nil-second-session-join coldest-temp-query)))
        "Failed expected counts when flipping insertion order for AccumulateWithJoinNode.")

    (is (= #{{:?cold (->Cold 10)}}
           (set (query insert-nil-first-session-join coldest-temp-query))
           (set (query insert-nil-second-session-join coldest-temp-query)))
        "Failed expected query results when flipping insertion order for AccumulateWithJoinNode.")))

(def-rules-test test-nil-accum-reduced-has-tokens-retracted-when-item-retracted

  {:rules [coldest-temp-rule-no-join  [[[?coldest <- maybe-nil-min-temp :from [Temperature]]]
                                       (insert! (->Cold (:temperature ?coldest)))]

           coldest-temp-rule-join [[[:max-threshold [{:keys [temperature]}]
                                              (= ?max-temp temperature)]

                                             ;; Note a non-equality based unification.
                                             ;; Gets max temp under a given max threshold.
                                             [?coldest <- maybe-nil-min-temp :from [Temperature
                                                                                    ;; Gracefully handle nil.
                                                                                    (< (or temperature 0)
                                                                                       ?max-temp)]]]

                                   (insert! (->Cold (:temperature ?coldest)))]]

   :queries [coldest-temp-query [[] [[?cold <- Cold]]]]

   :sessions [empty-session-no-join [coldest-temp-rule-no-join coldest-temp-query] {}
              empty-session-join [coldest-temp-rule-join coldest-temp-query] {}]}

  (let [nil-temp (->Temperature nil "MCI")

        session-no-join (-> empty-session-no-join
                            (insert nil-temp)
                            (retract nil-temp)
                            fire-rules)

        session-join (-> empty-session-join
                         (insert (with-meta {:temperature 10} {:type :max-threshold}))
                         (insert nil-temp)
                         (retract nil-temp)
                         fire-rules)]

    (is (empty? (set (query session-no-join coldest-temp-query)))
        "Failed expected empty query results for AccumulateNode.")

    (is (empty? (set (query session-join coldest-temp-query)))
        "Failed expected empty query results for AccumulateWithJoinNode.")))

(defn constant-accum [v]
  (accumulate
   :initial-value v
   :reduce-fn (constantly v)
   :combine-fn (constantly v)
   :retract-fn (constantly v)
   :convert-return-fn identity))

(def-rules-test test-constant-accum-bindings-downstream-accumulate-node

  {:rules [r1 [[[?result <- (constant-accum []) :from [Cold (= ?temperature temperature)]]]
               (insert! (->TemperatureHistory [?result ?temperature]))]

           r2 [[[Hot (= ?hot-temp temperature)]
                            [?result <- (constant-accum []) :from [Cold (< temperature ?hot-temp)
                                                                   (= ?temperature temperature)]]]
               (insert! (->TemperatureHistory [?result ?temperature]))]]

   :queries [q1 [[] [[TemperatureHistory (= ?temps temperatures)]]]]

   :sessions [empty-session-no-join [r1 q1] {}
              empty-session-join [r2 q1] {}]}

  (doseq [[empty-session node-type] [[empty-session-no-join
                                      "AccumulateNode"]
                                     [empty-session-join
                                      "AccumulateWithJoinFilterNode"]]]

    (is (= (-> empty-session
                 fire-rules
                 (insert (Cold. 10) (Hot. 100))
                 fire-rules
                 (query q1))
             [{:?temps [[] 10]}])
          (str "Single Cold fact with double firing for node type " node-type))

      (is (= (-> empty-session
                 (insert (Cold. 10) (Hot. 100))
                 fire-rules
                 (query q1))
             [{:?temps [[] 10]}])
          (str "Single Cold fact with single firing for node type " node-type))

      (is (= (-> empty-session
                 fire-rules
                 (insert (->Cold 10) (->Cold 20) (Hot. 100))
                 fire-rules
                 (query q1)
                 frequencies)
             {{:?temps [[] 10]} 1
              {:?temps [[] 20]} 1})
          (str "Two Cold facts with double firing for node type " node-type))

      (is (= (-> empty-session
                 fire-rules
                 (insert (->Cold 10) (->Cold 20) (Hot. 100))
                 fire-rules
                 (query q1)
                 frequencies)
             {{:?temps [[] 10]} 1
              {:?temps [[] 20]} 1})
          (str "Two Cold facts with single firing for node type " node-type))

      (is (= (-> empty-session
                 (insert (->Cold 10) (->Hot 100))
                 (fire-rules)
                 (retract (->Cold 10))
                 (fire-rules)
                 (query q1))
             [])
          (str "Retracting all elements that matched an accumulator with an initial value "
               \newline
               "should cause all facts downstream from the accumulator to be retracted when the accumulator creates binding groups."
               \newline
               "Accumulator node type: " node-type))))

(def-rules-test nil-accumulate-node-test

  {:rules [r1 [[[?r <- (constant-accum nil) :from [Cold]]]
               (insert! (->TemperatureHistory ?r))]

           r2 [[[Hot (= ?hot-temp temperature)]
                [?r <- (constant-accum nil) :from [Cold (< temperature ?hot-temp)]]]
               (insert! (->TemperatureHistory ?r))]]

   :queries [q [[] [[TemperatureHistory (= ?temps temperatures)]]]]

   :sessions [no-join-session [r1 q] {}
              join-session [r2 q] {}]}

  (doseq [[empty-session node-type] [[no-join-session
                                      "AccumulateNode"]
                                     [join-session
                                      "AccumulateWithJoinFilterNode"]]]
    (is (= (-> empty-session
               fire-rules
               (query q))
           [])
        (str "A nil initial value with no matching elements should not propagate from a " node-type))

    (is (= (-> empty-session
               (insert (->Cold 10))
               fire-rules
               (query q))
           [])
        (str "A nil initial value with matching elements should still not propagate if the final result is nil."))))

(def-rules-test test-multiple-minimum-accum-retractions

  {:rules [coldest-rule-1 [[[?coldest-temp <- (acc/min :temperature :returns-fact true) :from [ColdAndWindy]]]
                           (insert! (->Cold (:temperature ?coldest-temp)))]

           coldest-rule-2 [[[Hot (= ?max-temp temperature)]
                                        [?coldest-temp <- (acc/min :temperature :returns-fact true) :from [ColdAndWindy (< temperature ?max-temp)]]]
                           (insert! (->Cold (:temperature ?coldest-temp)))]]

   :queries [cold-query [[] [[Cold (= ?t temperature)]]]]

   :sessions [session-no-join [coldest-rule-1 cold-query] {}
              session-join [coldest-rule-2 cold-query] {}]}

  (doseq [[empty-session node-type]

            [[session-no-join "AccumulateNode"]
             [session-join "AccumulateWithJoinFilterNode"]]]

      ;; Note: in the tests below we deliberately insert or retract one fact at a time and then fire the rules.  The idea
      ;; is to verify that even accumulator activations and retractions that do not immediately change the tokens propagated
      ;; downstream are registered appropriately for the purpose of truth maintenance later.  This pattern ensures that we have
      ;; distinct activations and retractions.

      (let [all-temps-session (-> empty-session
                                  (insert (->ColdAndWindy 10 10) (->Hot 100))
                                  fire-rules
                                  ;; Even though the minimum won't change, the addition of an additional
                                  ;; fact with the same minimum changes how many of these facts can be retracted
                                  ;; before the minimum propagated downstream changes.
                                  (insert (->ColdAndWindy 10 10))
                                  fire-rules
                                  (insert (->ColdAndWindy 20 20))
                                  fire-rules)

            one-min-retracted-session (-> all-temps-session
                                          ;; Even though the minimum won't change after a single retraction, we now only need
                                          ;; one retraction to change the minimum rather than two.
                                          (retract (->ColdAndWindy 10 10))
                                          fire-rules)

            all-min-retracted  (-> one-min-retracted-session
                                   (retract (->ColdAndWindy 10 10))
                                   fire-rules)]

        (is (= (query all-temps-session cold-query)
               [{:?t 10}])
            (str "With all 3 ColdAndWindy facts in the session the minimum of 10 should be chosen for node type " node-type))

        (is (= (query one-min-retracted-session cold-query)
               [{:?t 10}])
            (str "With only one of the ColdAndWindy facts of a temperature of 10 retracted the minimum is still 10 for node type " node-type))

        (is (= (query all-min-retracted cold-query)
               [{:?t 20}])
            (str "With both ColdAndWindy facts with a temperature of 10, the new minimum is 20 for node type " node-type)))))

(def-rules-test test-no-retractions-of-nil-initial-value-accumulator-results

  {:queries [cold-query [[] [[Cold (= ?t temperature)]]]]

   :rules [coldest-temp-hash-join [[[Hot]
                                    [?coldest-temp <- (acc/min :temperature) :from [ColdAndWindy]]]
                                   (insert! (->Cold ?coldest-temp))]

           coldest-temp-filter-join [[[Hot (= ?max-temp temperature)]
                                      [?coldest-temp <- (acc/min :temperature) :from [ColdAndWindy (< temperature ?max-temp)]]]
                                     (insert! (->Cold ?coldest-temp))]]

   :sessions [session-hash-join [cold-query coldest-temp-hash-join] {}
              session-filter-join [cold-query coldest-temp-filter-join] {}]}

  (doseq [[empty-session node-type] [[session-hash-join "AccumulateNode"]
                                     [session-filter-join "AccumulateWithJoinFilterNode"]]]

    (is (empty? (-> empty-session
                    (insert (->Hot 100))
                    fire-rules
                    (query cold-query)))
        (str "There should be no Cold fact since the initial value is nil for node type " node-type))

    (is (= (-> empty-session
               (insert (->Cold nil) (->Hot 100))
               fire-rules
               (insert (->ColdAndWindy 10 10))
               fire-rules
               (query cold-query)
               frequencies)
           (frequencies [{:?t nil} {:?t 10}]))
        (str "We should not retract a nil initial value when the accumulator becomes met in case a fact with the relevant nil" \newline
             " is in the session for other reasons for node type" node-type))

    (is (= (-> empty-session
               (insert (->Cold nil) (->Hot 100))
               fire-rules
               (retract (->Hot 100))
               fire-rules
               (query cold-query))
           [{:?t nil}])
        (str "Calling left-retract on an accumulator node that has not previously propagated should not cause a retraction of " \newline
             "facts that could be in the session for other reasons."))))

(def nil-unsafe-accum-init-value-accum
  "This is intended for use in test-nil-initial-value-accum-with-unsafe-convert-return-fn."
  (acc/accum
   {:initial-value nil
    ;; Propagate whichever argument is not nil.
    :reduce-fn (fn [a b] (if a
                           a
                           b))
    ;; Propagate whichever argument is not nil.
    :combine-fn (fn [a b] (if a
                            a
                            b))
    :retract-fn (fn [& _] (throw (ex-info "This accumulator should not retract." {})))
    :convert-return-fn (fn [v]
                         (if (some? v)
                           v
                           (throw (ex-info "Convert-return-fn was called with nil." {}))))}))

;; The reason for this test is that nil initial values on accumulators prevent propagation of the initial value,
;; which meant prior to issue 182 that the convert-return-fn of an accumulator with a nil initial value need not
;; be nil-safe.
(def-rules-test test-nil-initial-value-accum-with-unsafe-convert-return-fn

  {:queries [cold-query [[] [[Cold (= ?t temperature)]]]]

   :rules [hash-join [[[Hot]
                       [?coldest-temp <- nil-unsafe-accum-init-value-accum :from [ColdAndWindy]]]
                      (insert! (->Cold ?coldest-temp))]

           filter-join [[[Hot (= ?max-temp temperature)]
                                                           [?coldest-temp <- nil-unsafe-accum-init-value-accum :from [ColdAndWindy (< temperature ?max-temp)]]]
                        (insert! (->Cold ?coldest-temp))]]

   :sessions [hash-join-session [hash-join cold-query] {}
              filter-join-session [filter-join cold-query] {}]}

  (doseq [[empty-session node-type] [[hash-join-session
                                      "AccumulateNode"]
                                     [filter-join-session
                                      "AccumulateWithJoinFilterNode"]]]
    (is (empty? (-> empty-session
                    (insert (->Hot 100))
                    fire-rules
                    (query cold-query)))
        (str "There should be no propagated value since the initial-value is nil for node type: " node-type))

    (is (= (-> empty-session
               (insert (->Hot 100) (->ColdAndWindy 10 10))
               fire-rules
               (query cold-query))
           [{:?t (->ColdAndWindy 10 10)}])
        (str "The convert-return-fn should not be called with nil as its argument even once something exists to propagate for node type: " node-type))))

(def-rules-test test-accum-without-change-in-result-no-downstream-propagation

  {:rules [r1 [[[?coldest-temp <- (acc/min :temperature) :from [Cold]]]
               (swap! side-effect-holder conj ?coldest-temp)]

           r2 [[[?lowest-hot <- (acc/min :temperature) :from [Hot]]
                [?coldest-temp <- (acc/min :temperature) :from [Cold (< temperature ?lowest-hot)]]]
               (swap! side-effect-holder conj ?coldest-temp)]]

   :sessions [s1 [r1] {}
              s2 [r2] {}]}

  (reset! side-effect-holder [])

  (-> s1
      (insert (->Cold 10))
      fire-rules
      (insert (->Cold 10))
      fire-rules)
  (is (= (frequencies @side-effect-holder)
         {10 1})
      "The minimum temperature accum should only propagate once through an AccumulateNode.")

  (reset! side-effect-holder [])

  (-> s2
      (insert (->Cold 10) (->Hot 100))
      fire-rules
      (insert (->Cold 10))
      fire-rules)
  (is (= (frequencies @side-effect-holder)
         {10 1})
      "The minimum temperature accum should only propagate once through an AccumulateWithJoinFilterNode."))

(def-rules-test test-accumulate-right-activate-then-right-retract-no-left-activate

  {:queries [q [[]
                [[Cold]
                 [?hs <- (assoc (acc/all)
                                ;; Add an explicit :convert-return-fn to be sure that this
                                ;; is only called with a valid reduced value.
                                :convert-return-fn
                                (fn [x]
                                  (if (coll? x)
                                    x
                                    (is false
                                        (str "An invalid value was given to the :convert-return-fn: " x)))))
                  :from [Hot]]]]]

   :sessions [empty-session [q] {}]}

  (is (empty? (-> empty-session
                  (insert (->Hot 50))
                  fire-rules
                  (retract (->Hot 50))
                  fire-rules
                  (query q)
                  set))))

(def-rules-test test-accumulate-left-retract-initial-value-new-bindings-token-add-and-remove

  {:rules [r1 [[[?w <- WindSpeed (= ?loc location)]
                            [?t <- (assoc (acc/all) :convert-return-fn (constantly []))
                             :from [Temperature (= ?loc location) (= ?degrees temperature)]]]
               (insert! (->TemperatureHistory [?loc ?degrees]))]

           r2 [[[?w <- WindSpeed (= ?loc location)]
                            [?t <- (assoc (acc/all) :convert-return-fn (constantly []))
                             ;; Note that only the binding that comes from a previous condition can use a filter function
                             ;; other than equality.  The = symbol is special-cased to potentially create a new binding;
                             ;; if we used (tu/join-filter-equals ?degrees temperature) here we would have an invalid rule constraint.
                             :from [Temperature (tu/join-filter-equals ?loc location) (= ?degrees temperature)]]]
               (insert! (->TemperatureHistory [?loc ?degrees]))]]

   :queries [q [[] [[TemperatureHistory (= ?ts temperatures)]]]]

   :sessions [s1 [r1 q] {}
              s2 [r2 q] {}]}
  (doseq [[empty-session join-type] [[s1 "simple hash join"]
                                     [s2 "filter join"]]]

    (is (= (-> empty-session
               (insert (->WindSpeed 10 "MCI") (->Temperature 20 "MCI"))
               fire-rules
               (query q))
           [{:?ts ["MCI" 20]}])
        (str "Basic sanity test of rules for a " join-type))

    (is (= (-> empty-session
               (insert (->WindSpeed 10 "MCI") (->Temperature 20 "MCI"))
               fire-rules
               (retract (->WindSpeed 10 "MCI"))
               fire-rules
               (insert (->WindSpeed 10 "MCI"))
               fire-rules
               (query q))
           [{:?ts ["MCI" 20]}])
        (str "Removing a token and reinserting it should leave element history intact when accumulating with a " join-type))))

(def-rules-test test-accumulate-with-bindings-from-parent

  {:rules [r1 [[[?w <- WindSpeed (= ?loc location)]
                            [?ts <- (acc/all) :from [Temperature (= ?loc location)]]]
               (insert! (->TemperatureHistory [?loc (map :temperature ?ts)]))]

           r2 [[[?w <- WindSpeed (= ?loc location)]
                            [?ts <- (acc/all) :from [Temperature (tu/join-filter-equals ?loc location)]]]
               (insert! (->TemperatureHistory [?loc (map :temperature ?ts)]))]]

   :queries [q [[] [[TemperatureHistory (= ?ts temperatures)]]]]

   :sessions [s1 [r1 q] {}
              s2 [r2 q] {}]}

  (doseq [[empty-session join-type] [[s1 "simple hash join"]
                                     [s2 "filter join"]]]

      (is (= (-> empty-session
                 (insert (->WindSpeed 10 "MCI"))
                 fire-rules
                 (query q))
             [{:?ts ["MCI" []]}])
          (str "Simple case of joining with an empty accumulator with binding from a parent" \newline
               "for a " join-type))

      (is (= (-> empty-session
                 (insert (->WindSpeed 10 "MCI") (->Temperature 20 "MCI"))
                 fire-rules
                 (query q))
             [{:?ts ["MCI" [20]]}])
          (str "Simple case of joining with a non-empty accumulator with a binding from a parent" \newline
               "for a " join-type))

      (is (= (-> empty-session
                 (insert (->WindSpeed 10 "MCI") (->Temperature 30 "MCI") (->Temperature 20 "LAX"))
                 fire-rules
                 (query q))
             [{:?ts ["MCI" [30]]}])
          (str "One value can join with parent node, but the other value has no matching parent " \newline
               "for a " join-type))

      (is (= (-> empty-session
                 (insert (->WindSpeed 10 "MCI") (->Temperature 20 "LAX"))
                 fire-rules
                 (query q))
             [{:?ts ["MCI" []]}])
          (str "Creation of a non-equal binding from a parent node " \newline
               "should not allow an accumulator to fire for another binding value for a " join-type))))

(def-rules-test test-accumulate-with-explicit-nil-binding-value

  {:rules [binding-from-self [[[?hot-facts <- (acc/all) :from [Hot (= ?t temperature)]]]
                              (insert! (->Temperature [?t []] "MCI"))]

           binding-from-parent [[[Cold (= ?t temperature)]
                                             [?hot-facts <- (acc/all) :from [Hot (= ?t temperature)]]]
                                (insert! (->Temperature [?t []] "MCI"))]

           binding-from-parent-non-equals [[[Cold (= ?t temperature)]
                                                        [?hot-facts <- (acc/all) :from [Hot (tu/join-filter-equals ?t temperature)]]]
                                           (insert! (->Temperature [?t []] "MCI"))]]

   :queries [q [[] [[Temperature (= ?t temperature)]]]]

   :sessions [s1 [binding-from-self q] {}
              s2 [binding-from-parent q] {}
              s3 [binding-from-parent-non-equals q] {}]}

  (is (= [{:?t [nil []]}]
           (-> s1
               (insert (->Hot nil))
               fire-rules
               (query q)))
      "An explicit value of nil in a field used to create a binding group should allow the binding to be created.")

  (doseq [[empty-session constraint-type] [[s2 "simple hash join"]
                                           [s3 "filter join"]]]
    (is (= [{:?t [nil []]}]
           (-> empty-session
               (insert (->Cold nil))
               fire-rules
               (query q)))
        (str "An explicit value of nil from a parent should allow the binding to be created for a " constraint-type))))

(def false-initial-value-accum (acc/accum
                                {:initial-value false
                                 ;; Propagate whichever argument is not nil.
                                 :reduce-fn (constantly ::dummy-value)
                                 ;; Propagate whichever argument is not nil.
                                 :combine-fn (constantly ::dummy-value)
                                 :retract-fn (constantly ::dummy-value)
                                 :convert-return-fn identity}))

(def-rules-test test-false-initial-value

  {:queries [t-history-query [[] [[TemperatureHistory (= ?ts temperatures)]]]]

   :rules [no-join [[[?t <- false-initial-value-accum :from [Cold]]]
                    (insert! (->TemperatureHistory ?t))]

           filter-join [[[?all-hot <- (acc/all) :from [Hot]]
                         [?t <- false-initial-value-accum :from [Cold (every? (fn [hot-fact]
                                                                                (< temperature (:temperature hot-fact)))
                                                                              ?all-hot)]]]
                        (insert! (->TemperatureHistory ?t))]]

   :sessions [empty-session-no-join [t-history-query no-join] {}
              empty-session-filter-join [t-history-query filter-join] {}]}

  (doseq [[node-type empty-session] [["AccumulateNode"
                                      empty-session-no-join]
                                     ["AccumulateWithJoinFilterNode"
                                      empty-session-filter-join]]]

    (is (= (-> empty-session
               fire-rules
               (query t-history-query))
           [{:?ts false}])
        (str "A false initial value should propagate for node type " node-type))))

;; Version of the "all" accumulator that converts the result to boolean false if it is empty.
(def false-if-empty-accum-all
  (let [base-all (acc/all)
        base-convert-return (:convert-return-fn base-all)
        wrapped-convert-return (fn [unconverted]
                                 (let [prelim-converted (base-convert-return unconverted)]
                                   (if (empty? prelim-converted)
                                     false
                                     prelim-converted)))]
    (assoc base-all :convert-return-fn wrapped-convert-return)))

(def-rules-test test-false-from-convert-return-fn

  ;;; Test the behavior when the converted return value of an accumulator is false,
  ;;; as opposed to the accumulated or retracted value.

  {:queries [t-history-query [[] [[TemperatureHistory (= ?ts temperatures)]]]]

   :rules [no-join [[[?t <- false-if-empty-accum-all :from [Cold]]]
                    (insert! (->TemperatureHistory ?t))]

           filter-join [[[?hot-temps <- (acc/all) :from [Hot]]
                         ;; This Cold condition is trivially true; we just need to force use of the
                         ;; AccumulateWithJoinFilterNode.
                         [?t <- false-if-empty-accum-all :from [Cold ((constantly true) ?hot-temps)]]]
                        (insert! (->TemperatureHistory ?t))]]

   :sessions [empty-session-no-join [no-join t-history-query] {}
              empty-session-filter-join [filter-join t-history-query] {}]}

  (doseq [[node-type empty-session] [["AccumulateNode"
                                      empty-session-no-join]
                                     ["AccumulateWithJoinFilterNode"
                                      empty-session-filter-join]]]

    (is (= (-> empty-session
               fire-rules
               (query t-history-query))
           [{:?ts false}])
        (str "A nil initial value created by the convert-return-fn should propagate from node type : " node-type))

    (is (= (-> empty-session
               (insert (->Cold 10))
               fire-rules
               (query t-history-query))
           [{:?ts [(->Cold 10)]}])
        (str "A Cold fact inserted before the first firing should propagate from node type: " node-type))

    (is (= (-> empty-session
               fire-rules
               (insert (->Cold 10))
               fire-rules
               (query t-history-query))
           [{:?ts [(->Cold 10)]}])
        (str "A Cold fact inserted after the first firing should propagate from node type: " node-type))

    (is (= (-> empty-session
               (insert (->Cold 10))
               fire-rules
               (retract (->Cold 10))
               fire-rules
               (query t-history-query))
           [{:?ts false}])
        (str "When a Cold fact is inserted and then retracted we should retract to the original value of false for node type: " node-type))))

(def false-safe-min-accum
  (#'acc/comparison-based :temperature
                          (fn [a b]
                            (cond (false? a) false
                                  (false? b) true
                                  :else (throw
                                         (ex-info (str "This test should only compare false and numeric values, "
                                                       \newline not
                                                       "a numeric value to another numeric value")
                                                  {:a a :b b}))))
                          false))

(def-rules-test test-false-field-in-accum

  {:queries [cold-and-windy-query [[] [[ColdAndWindy (= ?t temperature)]]]]

   :rules [cold-rule-no-join [[[?t <- false-safe-min-accum :from [Cold]]]
                              (insert! (->ColdAndWindy ?t ?t))]

           cold-rule-filter-join [[[?all-hot <- (acc/all) :from [Hot]]
                                   ;; This check is trivially true; we just need to force use of the
                                   ;; AccumulateWithJoinFilterNode.
                                   [?t <- false-safe-min-accum :from [Cold ((constantly true) ?all-hot)]]]
                                  (insert! (->ColdAndWindy ?t ?t))]]

   :sessions [empty-session-no-join [cold-and-windy-query cold-rule-no-join] {}
              empty-session-filter-join [cold-and-windy-query cold-rule-filter-join] {}]}

  (doseq [[empty-session node-type] [[empty-session-no-join "AccumulateNode"]
                                     [empty-session-filter-join "AccumulateWithJoinFilterNode"]]]

    (is (= (-> empty-session
               (insert (->Cold false))
               fire-rules
               (query cold-and-windy-query))
           [{:?t false}])
        (str "The minimum temperature should be boolean false for node type " node-type))

    (is (= (-> empty-session
               (insert (->Cold false) (->Cold 10))
               fire-rules
               (query cold-and-windy-query))
           [{:?t 10}])
        (str "The minimum temperature should be 10 for node type " node-type))

    (is (= (-> empty-session
               (insert (->Cold false) (->Cold 10))
               fire-rules
               (retract (->Cold 10))
               fire-rules
               (query cold-and-windy-query))
           [{:?t false}])
        (str "The minimum temperature should retract back to boolean false for node type " node-type))))

(def-rules-test test-accum-non-matching-element-no-ordering-impact
  ;; There was a problem where right-retract could cause the ordering of elements in the memory
  ;; to be altered even when the retracted element matched no tokens, and the AccumulateWithJoinFilterNode
  ;; was optimized to not perform any operations when a non-matching element was retracted.
  ;; When right-activate used the new memory to perform truth maintenance operations it therefore had an inaccurate
  ;; ordering of the previous state, so it would try to retract [A B] when what was actually present was [B A].
  ;; This would result in extra downstream tokens, since we'd have, for example, both [B A] and [B A C] when
  ;; it tried to retract [A B], nothing was done, and then propagated [A B C].  The objective of this test
  ;; is to detect such reorderings that have an impact on rules session outcomes.

  {:queries [q [[] [[TemperatureHistory (= ?ws temperatures)]]]]

   :rules [r1 [[[Cold (= ?t temperature)]
                            [?ws <- (acc/all) :from [ColdAndWindy
                                                     (and (tu/join-filter-equals ?t temperature)
                                                          (even? windspeed))]]]
               (insert! (->TemperatureHistory (map :windspeed ?ws)))]

           r2 [[[Cold (= ?t temperature)]
                            [?ws <- (acc/all) :from [ColdAndWindy (= ?t temperature)
                                                     (even? windspeed)]]]
               (insert! (->TemperatureHistory (map :windspeed ?ws)))]]

   :sessions [s1 [r1 q] {}
              s2 [r2 q] {}]}

  (doseq [[empty-session join-type] [[s1 "filter join"]
                                     [s2 "simple hash join"]]

            :let [non-matching-first (-> empty-session
                                          (insert (->ColdAndWindy 10 23))
                                          (insert (->ColdAndWindy 10 20))
                                          (insert (->ColdAndWindy 10 22))
                                          (insert (->Cold 10))
                                          fire-rules)

                  non-matching-middle (-> empty-session
                                          (insert (->ColdAndWindy 10 20))
                                          (insert (->ColdAndWindy 10 23))
                                          (insert (->ColdAndWindy 10 22))
                                          (insert (->Cold 10))
                                          fire-rules)

                  non-matching-last (-> empty-session
                                        (insert (->ColdAndWindy 10 20))
                                        (insert (->ColdAndWindy 10 22))
                                        (insert (->ColdAndWindy 10 23))
                                        (insert (->Cold 10))
                                        fire-rules)

                  [first-removed middle-removed last-removed] (for [init-session [non-matching-first
                                                                                  non-matching-middle
                                                                                  non-matching-last]]
                                                                (-> init-session
                                                                    (retract (->ColdAndWindy 10 23))
                                                                    ;; Perform an additional operation to reveal any discrepancy between the element ordering
                                                                    ;; in the candidates and the resulting ordering in downstream tokens.
                                                                    (insert (->ColdAndWindy 10 24))
                                                                    fire-rules))

                  without-rhs-ordering (fn [query-results]
                                         (map (fn [result]
                                                (update result :?ws set))
                                              query-results))]]

      (is (= [{:?ws #{20 22}}]
             ;; Put elements in a set to be independent of the ordering of acc/all in the assertions.
             ;; All we care about is that the cardinality of the query results is correct, which may
             ;; not be the case if the ordering is inconsistent between operations.  User code, however, needs
             ;; to be OK with getting any ordering in the RHS, so we just test that contract here.
             (without-rhs-ordering (query non-matching-first q))
             (without-rhs-ordering (query non-matching-middle q))
             (without-rhs-ordering (query non-matching-last q)))
          (str "Sanity test of our rule that does not exercise right-retract, the main purpose of this test, for a " join-type))

      (doseq [[final-session message] [[first-removed "first right activation"]
                                       [middle-removed "middle right activation"]
                                       [last-removed "last right activation"]]]
        (is (= [{:?ws #{20 22 24}}]
               (without-rhs-ordering (query final-session q)))
            (str "Retracting non-matching " message "for a " join-type)))))

(def min-accum-convert-return-fn-nil-to-zero (acc/min :temperature))

(def-rules-test test-accum-filter-or-retract-all-elements-with-convert-return-truthy-nil-initial-value

  {:queries [lousy-weather-query [[] [[LousyWeather]]]]

   :rules [min-non-cold [[[?min-cold <- (acc/min :temperature) :from [Cold]]
                                      [?min-temp <- (acc/min :temperature) :from [Temperature (< temperature ?min-cold)]]]
                         (insert! (->LousyWeather))]]

   :sessions [empty-session [lousy-weather-query min-non-cold] {}]}

      (is (empty? (-> empty-session
                    (insert (->Temperature 20 "MCI"))
                    (insert (->Cold 10))
                    fire-rules
                    (query lousy-weather-query)))
        "Insert non-matching element first then the token.")

    (is (empty? (-> empty-session
                    (insert (->Cold 10))
                    (insert (->Temperature 20 "MCI"))
                    fire-rules
                    (query lousy-weather-query)))
        "Insert token first then non-matching element.")

    (is (empty? (-> empty-session
                    (insert (->Cold 10) (->Temperature 20 "MCI"))
                    fire-rules
                    (retract (->Temperature 20 "MCI"))
                    fire-rules
                    (query lousy-weather-query)))
        "Insert facts matching both conditions first and then remove a Temperature fact, thus causing the accumulator on Temperature to
         right-retract back to its initial value.  Since the initial value is nil the activation of the rule should be removed."))

(def-rules-test test-initial-value-used-when-non-nil-and-new-binding-group-created
  ;; Validate that the initial value is used when a new binding group is created and no result is propagated without
  ;; elements present.  The count accumulator has a non-nil initial value of the number 0.  Any attempt to add to
  ;; nil would result in an exception, so this test validates that 0 was used as the initial value rather than nil.

  {:queries [q [[] [[TemperatureHistory (= ?ts temperatures)]]]]

   :rules [hash-join-rule [[[WindSpeed (= ?loc location)]
                                        [?temp-count <- (acc/count) :from [Temperature (= ?loc location) (= ?temp temperature)]]]
                           (insert! (->TemperatureHistory [?loc ?temp ?temp-count]))]

           filter-join-rule [[[WindSpeed (= ?loc location)]
                              [?temp-count <- (acc/count) :from [Temperature (tu/join-filter-equals ?loc location) (= ?temp temperature)]]]
                             (insert! (->TemperatureHistory [?loc ?temp ?temp-count]))]]

   :sessions [empty-session-hash-join [q hash-join-rule] {}
              empty-session-filter-join [q filter-join-rule] {}]}

  (doseq [[empty-session join-type] [[empty-session-hash-join
                                      "simple hash join"]
                                     [empty-session-filter-join
                                      "join filter"]]]

      (is (= (-> empty-session
                 (insert (->WindSpeed 10 "MCI"))
                 fire-rules
                 (insert (->Temperature 20 "MCI"))
                 fire-rules
                 (query q))
             [{:?ts ["MCI" 20 1]}])
          (str "Inserting a WindSpeed and then later a matching Temperature for join type " join-type))))

(def-rules-test test-retract-of-fact-matching-accumulator-causes-downstream-retraction

  {:rules [create-cold [[[?cs <- (acc/all) :from [ColdAndWindy]]]
                                    (doseq [c ?cs]
                                      (insert! (->Cold (:temperature c))))]

           temp-from-cold [[[Cold (= ?t temperature)]]
                           (insert! (->Temperature ?t "MCI"))]]

   :queries [cold-query [[] [[Cold (= ?t temperature)]]]

             temp-query [[] [[Temperature (= ?t temperature)]]]]

   :sessions [empty-session [create-cold temp-from-cold
                             cold-query temp-query] {}]}

  (let [two-cold-windy-session (-> empty-session
                                   (insert (->ColdAndWindy 10 10)
                                           (->ColdAndWindy 20 20))
                                   fire-rules)

        one-cold-windy-retracted (-> two-cold-windy-session
                                     (retract (->ColdAndWindy 10 10))
                                     fire-rules)

        one-cold-windy-added (-> two-cold-windy-session
                                 (insert (->ColdAndWindy 15 15))
                                 fire-rules)]

    (is (= (frequencies (query two-cold-windy-session cold-query))
           (frequencies (query two-cold-windy-session temp-query))
           (frequencies [{:?t 10} {:?t 20}]))
        "Sanity test without retractions or insertions involved")

    (is (= (frequencies (query one-cold-windy-retracted cold-query))
           (frequencies (query one-cold-windy-retracted temp-query))
           (frequencies [{:?t 20}]))
        (str "When a fact that matched an accumulator condition is externally retracted "
             "both direct insertions and downstream ones should be changed to reflect this."))

    (is (= (frequencies (query one-cold-windy-added cold-query))
           (frequencies (query one-cold-windy-added temp-query))
           (frequencies [{:?t 10} {:?t 15} {:?t 20}]))
        (str "When a fact that matched an accumulator condition is externally added "
             "both direct insertions and downstream ones should be changed to reflect this."))))
