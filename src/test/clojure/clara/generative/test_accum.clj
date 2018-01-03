(ns clara.generative.test-accum
  (:require [clara.rules :refer :all]
            [clojure.test :refer :all]
            [clara.rules.testfacts :refer :all]
            [clara.rules.accumulators :as acc]
            [clara.rules.dsl :as dsl]
            [schema.test]
            [clara.generative.generators :as gen])
  (:import [clara.rules.testfacts
            Temperature
            WindSpeed
            Cold
            TemperatureHistory
            ColdAndWindy]))

(use-fixtures :once schema.test/validate-schemas)

(deftest test-simple-all-condition-binding-groups
  (let [r (dsl/parse-rule [[?ts <- (acc/all) :from [Temperature (= ?loc location)]]]
                          ;; The all accumulator can return facts in different orders, so we sort
                          ;; the temperatures to make asserting on the output easier.
                          (insert! (->TemperatureHistory [?loc (sort (map :temperature ?ts))])))

        q (dsl/parse-query [] [[?history <- TemperatureHistory]])

        empty-session (mk-session [r q] :cache false)]

    (let [operations [{:type :insert
                       :facts [(->Temperature 11 "MCI")]}
                      {:type :insert
                       :facts [(->Temperature 19 "MCI")]}
                      {:type :insert
                       :facts [(->Temperature 1 "ORD")]}
                      {:type :insert
                       :facts [(->Temperature 22 "LAX")]}
                      {:type :retract
                       :facts [(->Temperature 22 "LAX")]}
                      ;; Note that a :fire operation is added to the end later.  If this is at the end,
                      ;; then it should be functionally the same as if we only had one at the end.
                      ;; On the other hand, a fire operation in the middle of the operations could potentially
                      ;; reveal bugs.
                      {:type :fire}]

          operation-permutations (gen/ops->permutations operations {})

          expected-output? (fn [session permutation]
                             (let [actual-temp-hist (-> session
                                                        (query q)
                                                        frequencies)
                                   expected-temp-hist (frequencies [{:?history (->TemperatureHistory ["MCI" [11 19]])}
                                                                    {:?history (->TemperatureHistory ["ORD" [1]])}])]
                               (= actual-temp-hist expected-temp-hist)))]
      
      (doseq [permutation (map #(concat % [{:type :fire}]) operation-permutations)
              :let [session (gen/session-run-ops empty-session permutation)]]
        (is (expected-output? session permutation)
            (str "Failure for operation permutation: "
                 \newline
                 ;; Put into a vector so that the str implementation shows the elements of the collection,
                 ;; not just LazySeq.
                 (into [] permutation)
                 \newline
                 "Output was: "
                 \newline
                 (into [] (query session q))))))

    (let [operations (mapcat (fn [fact]
                               [{:type :insert
                                 :facts [fact]}
                                {:type :retract
                                 :facts [fact]}])
                             [(->Temperature 10 "MCI")
                              (->Temperature 20 "MCI")
                              (->Temperature 15 "LGA")
                              (->Temperature 25 "LGA")])

          operation-permutations (gen/ops->permutations operations {})]
      
      (doseq [permutation (map #(concat % [{:type :fire}]) operation-permutations)
              :let [session (gen/session-run-ops empty-session permutation)
                    output (query session q)]]
        (is (empty? output)
            (str "Non-empty result for operation permutation: "
                 \newline
                 (into [] permutation)
                 "Output was: "
                 (into [] output)))))))

(deftest test-min-accum-with-binding-groups
  (let [coldest-rule (dsl/parse-rule [[?coldest-temp <- (acc/min :temperature :returns-fact true)
                                       :from [ColdAndWindy (= ?w windspeed)]]]
                                     (insert! (->Cold (:temperature ?coldest-temp))))
        cold-query (dsl/parse-query [] [[?c <- Cold]])

        empty-session (mk-session [coldest-rule cold-query] :cache false)

        operations [{:type :insert
                     :facts [(->ColdAndWindy 10 20)]}
                    {:type :insert
                     :facts [(->ColdAndWindy 5 20)]}
                    {:type :retract
                     :facts [(->ColdAndWindy 5 20)]}
                    {:type :insert
                     :facts [(->ColdAndWindy 20 20)]}
                    {:type :insert
                     :facts [(->ColdAndWindy 0 30)]}
                    {:type :fire}]

        operation-permutations (gen/ops->permutations operations {})]

    (doseq [permutation (map #(concat % [{:type :fire}]) operation-permutations)
            :let [session (gen/session-run-ops empty-session permutation)
                  output (query session cold-query)]]
      (is (= (frequencies output)
             {{:?c (->Cold 0)} 1
              {:?c (->Cold 10)} 1})
          (str "The minimum cold temperatures per windspeed are not correct for permutation: "
               \newline
               (into [] permutation)
               \newline
               "The output was: "
               (into [] output))))))

(deftest test-min-accum-without-binding-groups
  (let [coldest-rule (dsl/parse-rule [[?coldest <- (acc/min :temperature) :from [Cold]]]
                                     (insert! (->Temperature ?coldest "MCI")))
        temp-query (dsl/parse-query [] [[Temperature (= ?t temperature)]])

        empty-session (mk-session [coldest-rule temp-query] :cache false)]

    (doseq [temp-1 (range 5)
            temp-2 (range 5)
            temp-3 (range 5)

            :let [operations [{:type :insert
                               :facts [(->Cold temp-1)]}
                              {:type :insert
                               :facts [(->Cold temp-2)]}
                              {:type :insert
                               :facts [(->Cold temp-3)]}
                              {:type :retract
                               :facts [(->Cold temp-3)]}
                              {:type :fire}]]

            permutation (map #(concat % [{:type :fire}])
                             (gen/ops->permutations operations {}))

            :let [session (gen/session-run-ops empty-session permutation)
                  output (query session temp-query)]]
      
      (is (= output
             [{:?t (min temp-1 temp-2)}])
          (str "Did not find the correct minimum temperature for permutation: "
               \newline
               (into [] permutation)
               \newline
               "Output was: "
               \newline
               (into [] output))))))
