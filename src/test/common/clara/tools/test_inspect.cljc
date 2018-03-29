(ns clara.tools.test-inspect
  (:require [clara.tools.testing-utils :as tu]
            [clara.tools.inspect :refer [inspect map->Explanation explain-activations with-full-logging without-full-logging]]
            [clara.rules :refer [insert fire-rules insert! insert-unconditional! retract query]]
            [clara.rules.accumulators :as acc]
            [schema.test :as st]
            [clojure.walk :as w]
    #?(:cljs [goog.string :as gstr])
    #?(:clj [clojure.test :refer [is deftest run-tests testing use-fixtures]]
       :cljs [cljs.test :refer-macros [is deftest run-tests testing use-fixtures]])
    #?(:clj [clara.rules.testfacts :refer :all]
       :cljs [clara.rules.testfacts :refer [Temperature TemperatureHistory
                                            WindSpeed Cold Hot ColdAndWindy LousyWeather
                                            First Second Third Fourth
                                            map->Cold map->Hot
                                            ->Temperature ->TemperatureHistory
                                            ->WindSpeed ->Cold ->Hot ->ColdAndWindy ->LousyWeather
                                            ->First ->Second ->Third ->Fourth]]))
  #?(:clj
     (:import [clara.rules.testfacts Temperature TemperatureHistory
                                     WindSpeed Cold Hot ColdAndWindy LousyWeather
                                     First Second Third Fourth])))

(use-fixtures :once schema.test/validate-schemas)

(tu/def-rules-test test-simple-inspect
  {:rules    [cold-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                         (insert! (map->Cold {:temperature :too-cold}))]
              hot-rule [[[Temperature (> temperature 80) (= ?t temperature)]]
                        (insert! (map->Hot {:temperature ?t}))]]
   :queries  [cold-query [[] [[Temperature (< temperature 20) (= ?t temperature)]]]]
   :sessions [empty-session [cold-query cold-rule hot-rule] {}]}

  ;; Create some rules and a session for our test.
  (let [hot-rule-90-explanation (map->Explanation {:matches  [{:fact      (->Temperature 90 "MCI")
                                                               :condition (first (:lhs hot-rule))}],
                                                   :bindings {:?t 90}})

        cold-rule-15-explanation (map->Explanation {:matches  [{:fact      (->Temperature 15 "MCI")
                                                                :condition (first (:lhs cold-query))}],

                                                    :bindings {:?t 15}})

        cold-rule-10-explanation (map->Explanation {:matches  [{:fact      (->Temperature 10 "MCI")
                                                                :condition (first (:lhs cold-query))}],
                                                    :bindings {:?t 10}})]

    (let [session (-> empty-session
                      with-full-logging
                      (insert (->Temperature 15 "MCI"))
                      (insert (->Temperature 10 "MCI"))
                      (insert (->Temperature 90 "MCI"))
                      (fire-rules))

          rule-dump (inspect session)]

      ;; Retrieve the tokens matching the cold query. This test validates
      ;; the tokens contain the expected matching conditions by retrieving
      ;; them directly from the query in question.
      (is (= (frequencies [cold-rule-15-explanation cold-rule-10-explanation])
             (frequencies (get-in rule-dump [:query-matches cold-query])))
          "Query matches test")

      ;; Retrieve tokens matching the hot rule.
      (is (= [hot-rule-90-explanation]
             ;; This validates that :unfiltered-rule-matches has activations that triggered logical insertions as
             ;; well.
             (get-in rule-dump [:unfiltered-rule-matches hot-rule])
             (get-in rule-dump [:rule-matches hot-rule]))
          "Rule matches test")

      (is (= [{:explanation hot-rule-90-explanation
               :fact        (map->Hot {:temperature 90})}]
             (get-in rule-dump [:insertions hot-rule]))
          "Insertions test")

      ;; Ensure the first condition in the rule matches the expected facts.
      (is (= (frequencies [(->Temperature 15 "MCI") (->Temperature 10 "MCI")])
             (frequencies (get-in rule-dump [:condition-matches (first (:lhs cold-rule))])))
          "Condition matches test")

      ;; Test the :fact->explanations key in the inspected session data.
      (is (= {(map->Cold {:temperature :too-cold}) [{:explanation cold-rule-10-explanation
                                                     :rule        cold-rule}
                                                    {:explanation cold-rule-15-explanation
                                                     :rule        cold-rule}]
              (map->Hot {:temperature 90})         [{:explanation hot-rule-90-explanation
                                                     :rule        hot-rule}]}

             ;; Avoid dependence on the ordering of the cold explanations.
             (update (:fact->explanations rule-dump)
                     (map->Cold {:temperature :too-cold})

                     (fn [expls]
                       (sort-by #(get-in % [:explanation :bindings :?t])
                                expls))))
          "Fact to explanations test"))

    (let [dup-inspected (-> empty-session
                            (insert (->Temperature 15 "MCI"))
                            (insert (->Temperature 15 "MCI"))
                            (fire-rules)
                            inspect)]
      (is (= (get-in dup-inspected [:insertions cold-rule])
             [{:explanation cold-rule-15-explanation
               :fact        (->Cold :too-cold)}
              {:explanation cold-rule-15-explanation
               :fact        (->Cold :too-cold)}])
          "When two identical facts cause two identical insertions there should be exactly 2 insertions for that rule
           in the inspected session.")
      (is (= (get-in dup-inspected [:fact->explanations (->Cold :too-cold)])
             [{:explanation cold-rule-15-explanation
               :rule        cold-rule}
              {:explanation cold-rule-15-explanation
               :rule        cold-rule}]))
      "When two identical facts cause two identical insertions there should be exactly 2 insertions under
      that fact in fact->explanations.")))

(def lowest-temp (acc/min :temperature :returns-fact true))

(tu/def-rules-test test-accum-inspect
  {:queries  [coldest-query [[] [[?t <- lowest-temp from [Temperature]]]]]
   :sessions [empty-session [coldest-query] {}]}
  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)]

    (let [accum-condition (-> coldest-query :lhs first (select-keys [:accumulator :from]))
          query-explanations (-> (inspect session) (:query-matches) (get coldest-query))]

      (is (= [(map->Explanation {:matches  [{:fact              (->Temperature 10 "MCI")
                                             :facts-accumulated [(->Temperature 15 "MCI")
                                                                 (->Temperature 10 "MCI")
                                                                 (->Temperature 80 "MCI")]
                                             :condition         accum-condition}]
                                 :bindings {:?t (->Temperature 10 "MCI")}})]
             query-explanations)))))

(tu/def-rules-test test-accum-join-inspect
  ;; Get the coldest temperature at MCI that is warmer than the temperature in STL.
  {:queries  [colder-query [[] [[Temperature (= "STL" location) (= ?stl-temperature temperature)]
                                [?t <- lowest-temp from [Temperature (> temperature ?stl-temperature)
                                                         (= "MCI location")]]]]]
   :sessions [empty-session [colder-query] {}]}
  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 20 "STL"))
                    (insert (->Temperature 80 "MCI"))
                    (insert (->Temperature 25 "MCI"))
                    fire-rules)]

    (let [matches (-> (inspect session)
                      :query-matches
                      (get colder-query)
                      first
                      :matches)

          conditions-matched (map :condition matches)]
      (doseq [condition conditions-matched]

        ;; The accumulator condition should have two constraints.
        (when (:accumulator condition)
          (is (= 2 (-> condition :from :constraints (count)))))))))

(tu/def-rules-test test-extract-test-inspect
  {:queries  [distinct-temps-query [[] [[Temperature (< temperature 20)
                                         (= ?t1 temperature)]

                                        [Temperature (< temperature 20)
                                         (= ?t2 temperature)
                                         (< ?t1 temperature)]]]]
   :sessions [empty-session [distinct-temps-query] {:cache false}]}
  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)]

    ;; Ensure that no returned contraint includes a generated variable name.
    (doseq [{:keys [matches bindings]} (-> (inspect session) :query-matches (get distinct-temps-query))
            constraints (map (comp :constraints :condition) matches)
            term (flatten constraints)]
      (is (not (and (symbol? term)
                    #?(:clj (.startsWith (name term) "?__gen"))
                    #?(:cljs (gstr/startsWith (name term) "?__gen"))))))))

(defn session->accumulated-facts-map
  "Given a session, return a map of logically inserted facts to any accumulated-over facts
   that justify that insertion"
  [session]
  (let [;; These "count is 1" requirements could be loosened if needed but they simplify
        ;; the tests.  The actual schemas allow these things to be sequences, and this
        ;; is completely valid, but we probably don't need situations that would lead them
        ;; to be sequences to test the functionality added in
        ;; https://github.com/cerner/clara-rules/issues/276 and this avoids needing to either
        ;; filter the sequences according to the semantics of each test case or weakening
        ;; test cases by just testing against the first thing in each sequence and ignoring
        ;; any (erroneously present) later elements.
        fail-on-multiple-insertions (fn [explanations]
                                      (if (< 1 (count explanations))
                                        ;; Since we expect this function to be called in the scope
                                        ;; of a test case use a false assertion rather than throwing
                                        ;; an exception so that the test fails
                                        ;; rather than reporting an error.
                                        (is false
                                            (str "There should be only one "
                                                 "insertion of a given fact in these tests"))
                                        explanations))

        fail-on-multiple-accum-conditions (fn [matches]
                                            (if (< 1 (count matches))
                                              (is false
                                                  (str "There should only be one accumulator "
                                                       "condition in these tests"))
                                              matches))]

    (as-> session x
          (inspect x)
          (:fact->explanations x)
          (into {}
                (map (fn [[k v]]
                       [k (->> v
                               fail-on-multiple-insertions
                               first
                               :explanation
                               :matches
                               (filter (comp :accumulator :condition))
                               fail-on-multiple-accum-conditions
                               first
                               :facts-accumulated
                               frequencies)]))
                x))))

(tu/def-rules-test test-get-matching-accum-facts-with-no-previous-conditions-and-new-binding
  {:rules    [min-freezing-at-loc-rule [[[?min <- (acc/min :temperature) from [Temperature (< temperature 0) (= ?loc location)]]]
                                        (insert! (->Cold ?min))]]
   :queries  [cold-query [[] [[Cold (= ?t temperature)]]]]
   :sessions [empty-session [min-freezing-at-loc-rule cold-query] {:cache false}]}
  (let [fired-session (-> empty-session
                          (insert (->Temperature 20 "MCI")
                                  (->Temperature -20 "MCI")
                                  (->Temperature -5 "ORD"))
                          fire-rules)]

    (is (= (frequencies (query fired-session cold-query))
           {{:?t -20} 1
            {:?t -5}  1})
        "Sanity check of the facts inserted")

    (is (= (session->accumulated-facts-map fired-session)
           {(->Cold -20) {(->Temperature -20 "MCI") 1}
            (->Cold -5)  {(->Temperature -5 "ORD") 1}}))))

(tu/def-rules-test test-get-matching-accum-facts-with-no-previous-conditions
  {:rules    [min-freezing-at-loc-rule [[[?min <- (acc/min :temperature) from [Temperature (< temperature 0)]]]
                                        (insert! (->Cold ?min))]]
   :queries  [cold-query [[] [[Cold (= ?t temperature)]]]]
   :sessions [empty-session [min-freezing-at-loc-rule cold-query] {:cache false}]}
  (let [fired-session (-> empty-session
                          (insert (->Temperature 20 "MCI")
                                  (->Temperature -20 "MCI")
                                  (->Temperature -5 "ORD"))
                          fire-rules)]

    (is (= (frequencies (query fired-session cold-query))
           {{:?t -20} 1})
        "Sanity check of the facts inserted")

    (is (= (session->accumulated-facts-map fired-session)
           {(->Cold -20) {(->Temperature -20 "MCI") 1
                          (->Temperature -5 "ORD")  1}}))))

(tu/def-rules-test test-get-matching-accum-facts-join-with-previous-no-new-bindings
  {:rules [windspeed-with-temps-simple-join
           [[[?w <- WindSpeed (= ?loc location)]
             [?temps <- (acc/all) from [Temperature (= ?loc location)]]]
            (insert! (->TemperatureHistory [?loc (->> ?temps
                                                      (map :temperature)
                                                      frequencies)]))]

           windspeed-with-temps-simple-join-unused-previous-binding
           [[[?w <- WindSpeed (= ?loc location) (= ?windspeed windspeed)]
             [?temps <- (acc/all) from [Temperature (= ?loc location)]]]
            (insert! (->TemperatureHistory [?loc (->> ?temps
                                                      (map :temperature)
                                                      frequencies)]))]

           windspeed-with-temps-simple-join-subsequent-binding
           [[[?temps <- (acc/all) from [Temperature (= ?loc location)]]
             [?w <- WindSpeed (= ?loc location) (= ?windspeed windspeed)]]
            (insert! (->TemperatureHistory [?loc (->> ?temps
                                                      (map :temperature)
                                                      frequencies)]))]

           windspeed-with-temps-complex-join
           [[[?w <- WindSpeed (= ?loc location)]
             [?temps <- (acc/all) from [Temperature (tu/join-filter-equals ?loc location)]]]
            (insert! (->TemperatureHistory [?loc (->> ?temps
                                                      (map :temperature)
                                                      frequencies)]))]
           windspeed-with-temps-complex-join-unused-previous-binding
           [[[?w <- WindSpeed (= ?loc location) (= ?windspeed windspeed)]
             [?temps <- (acc/all) from [Temperature (tu/join-filter-equals ?loc location)]]]
            (insert! (->TemperatureHistory [?loc (->> ?temps
                                                      (map :temperature)
                                                      frequencies)]))]
           windspeed-with-temps-complex-join-subsequent-binding
           [[[?temps <- (acc/all) from [Temperature (tu/join-filter-equals ?loc location)]]
             [?w <- WindSpeed (= ?loc location) (= ?windspeed windspeed)]]
            (insert! (->TemperatureHistory [?loc (->> ?temps
                                                      (map :temperature)
                                                      frequencies)]))]]
   :queries [temp-history-query [[] [[TemperatureHistory (= ?temps temperatures)]]]]
   :sessions [empty-with-temps-simple-join [windspeed-with-temps-simple-join
                                            temp-history-query] {:cache false}
              empty-with-temps-simple-join-unused-previous-binding [windspeed-with-temps-simple-join-unused-previous-binding
                                                                    temp-history-query] {:cache false}
              empty-with-temps-simple-join-subsequent-binding [windspeed-with-temps-simple-join-subsequent-binding
                                                               temp-history-query] {:cache false}
              empty-with-temps-complex-join [windspeed-with-temps-complex-join
                                             temp-history-query] {:cache false}
              empty-with-temps-complex-join-unused-previous-binding [windspeed-with-temps-complex-join-unused-previous-binding
                                                                     temp-history-query] {:cache false}
              empty-with-temps-complex-join-subsequent-binding [windspeed-with-temps-complex-join-subsequent-binding
                                                                 temp-history-query] {:cache false}]}
  (doseq [[empty-session
           join-type
           unused-previous-binding
           subsequent-binding] [[empty-with-temps-simple-join
                                 "Simple join"
                                 false
                                 false]

                                [empty-with-temps-simple-join-unused-previous-binding
                                 "Simple join"
                                 true
                                 false]

                                [empty-with-temps-simple-join-subsequent-binding
                                 "Simple join"
                                 false
                                 true]

                                [empty-with-temps-complex-join
                                 "Complex join"
                                 false
                                 false]

                                [empty-with-temps-complex-join-unused-previous-binding
                                 "Complex join"
                                 true
                                 false]

                                [empty-with-temps-complex-join-subsequent-binding
                                 "Complex join"
                                 false
                                 true]]

          :let [fired-session (-> empty-session
                                  (insert (->WindSpeed 20 "MCI")
                                          (->Temperature 10 "MCI")
                                          (->Temperature 0 "MCI")
                                          (->WindSpeed 50 "JFK")
                                          (->Temperature -30 "JFK"))
                                  fire-rules)]]

    (is (= (-> fired-session
               (query temp-history-query)
               frequencies)
           {{:?temps ["MCI" (frequencies [10 0])]} 1
            {:?temps ["JFK" (frequencies [-30])]}  1})
        (str "Sanity check for join type: "
             join-type
             " with unused previous binding: "
             unused-previous-binding
             " and subsequent binding: "
             subsequent-binding))

    (is (= (session->accumulated-facts-map fired-session)
           {(->TemperatureHistory ["MCI" (frequencies [10 0])])
            {(->Temperature 10 "MCI") 1
             (->Temperature 0 "MCI")  1}

            (->TemperatureHistory ["JFK" (frequencies [-30])])
            {(->Temperature -30 "JFK") 1}})
        (str "Check the accumulated facts for join type: "
             join-type
             " with unused previous binding: "
             unused-previous-binding
             " and subsequent binding: "
             subsequent-binding))))

(tu/def-rules-test test-inspect-retracted-fact
  {:queries [temp-query [[] [[Temperature (= ?t temperature)]]]]
   :sessions [empty-session [temp-query] {:cache false}]}
  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    fire-rules
                    (retract (->Temperature 10 "MCI"))
                    fire-rules)
        condition-matching-facts (-> session
                                     inspect
                                     :condition-matches
                                     vals)

        query-matches (-> session
                          inspect
                          :query-matches
                          vals)]

    (is (every? empty? query-matches))
    (is (every? empty? condition-matching-facts))))

;; Remove the original-constraints vs constraints distinction for ease of testing.
;; Note that this distinction is present in the FactCondition schema used by the compiler:
;; https://github.com/cerner/clara-rules/blob/0.15.2/src/main/clojure/clara/rules/schema.clj#L28
;; so the :original-constraints key that is added by the compiler and is absent
;; from the original rule condition was present before the preliminary refactoring for issue 307.
;;
;; TODO: This is a separate conversation from the refactoring to avoid the compiler dependency,
;; but there is an argument to be made that we should transform the :condition-matches in this
;; way prior to their return to the user in the inspect function, not just in tests here.
;; If we do so this transformation in the tests should be removed.
(defn original-constraints->constraints
  [condition-matches]
  (let [rename-fn (fn [form]
                    (if
                      (and
                        (map? form)
                        (contains? form :original-constraints))
                      (-> form
                          (dissoc :original-constraints)
                          (assoc :constraints (:original-constraints form)))
                      form))]
    (w/prewalk rename-fn condition-matches)))

(tu/def-rules-test test-negation-condition-matches
  {:rules [not-cold-rule-join [[[ColdAndWindy (= ?t temperature)]
                                ;; Note that it doesn't actually matter for the purpose of the
                                ;; :condition-matches if the join condition is satisfied; we're only
                                ;; looking at the constraints that are only on the Temperature and not
                                ;; part of a join.  However, adding a join condition forces the use of
                                ;; NegationWithJoinFilterNode so we test against that as well as
                                ;; NegationNode.
                                [:not [Temperature (tu/join-filter-equals temperature ?t)
                                       (< temperature 0)]]]
                               (insert! (->Hot :from-join))]
           not-cold-rule [[[:not [Temperature (< temperature 0)]]]
                          (insert! (->Hot :unknown))]]
   :sessions [empty-session [not-cold-rule not-cold-rule-join] {:cache false}]}
  (let [with-matching-fact-session (-> empty-session
                                       (insert (->Temperature -10 "MCI") (->ColdAndWindy 0 0))
                                       fire-rules)

        with-non-matching-fact-session (-> empty-session
                                           (insert (->Temperature 10 "MCI") (->ColdAndWindy 10 10))
                                           fire-rules)]

    (is (= []
           (-> empty-session inspect :condition-matches (get (-> not-cold-rule :lhs first)))
           (-> with-non-matching-fact-session inspect :condition-matches (get (-> not-cold-rule :lhs first)))))

    (is (= [(->Temperature -10 "MCI")]
           (-> with-matching-fact-session inspect :condition-matches (get (-> not-cold-rule :lhs first)))))

    (is (= []
           (-> empty-session inspect :condition-matches original-constraints->constraints
               (get (-> not-cold-rule-join :lhs second)))
           (-> with-non-matching-fact-session inspect :condition-matches original-constraints->constraints
               (get (-> not-cold-rule-join :lhs second)))))

    (is (= [(->Temperature -10 "MCI")]
           (-> with-matching-fact-session inspect :condition-matches original-constraints->constraints
               (get (-> not-cold-rule-join :lhs second)))))))

(tu/def-rules-test test-condition-matches-on-join-conditions

  {:rules    [simple-join [[[Temperature (= ?loc location) (= ?temp temperature)]
                            [WindSpeed (= ?loc location) (= ?windspeed windspeed)]]
                           (insert! (->ColdAndWindy ?temp ?windspeed))]

              complex-join [[[Temperature (= ?loc location) (= ?temp temperature)]
                             [WindSpeed (tu/join-filter-equals ?loc location) (= ?windspeed windspeed)]]
                            (insert! (->ColdAndWindy ?temp ?windspeed))]]

   :queries  [cold-windy-query [[] [[ColdAndWindy (= ?t temperature) (= ?w windspeed)]]]]

   :sessions [empty-simple-join [simple-join cold-windy-query] {}
              empty-complex-join [complex-join cold-windy-query] {}]}

  (let [simple-successful-join (-> empty-simple-join
                                   (insert (->Temperature 0 "MCI") (->WindSpeed 50 "MCI"))
                                   fire-rules)

        complex-successful-join (-> empty-complex-join
                                    (insert (->Temperature 0 "MCI") (->WindSpeed 50 "MCI"))
                                    fire-rules)

        simple-failed-join (-> empty-simple-join
                               (insert (->Temperature 0 "ORD") (->WindSpeed 50 "MCI"))
                               fire-rules)

        complex-failed-join (-> empty-complex-join
                                (insert (->Temperature 0 "ORD") (->WindSpeed 50 "MCI"))
                                fire-rules)

        get-condition-match (fn [session fact-type]
                              (let [matching-entries (->> session
                                                          inspect
                                                          :condition-matches
                                                          (filter (fn [[k v]] (= (:type k)
                                                                                 fact-type))))]
                                (if (> (count matching-entries) 1)
                                  (throw (ex-info "Found multiple matches of type"
                                                  {:session   session
                                                   :fact-type fact-type}))
                                  (some-> matching-entries first val))))]

    (is (= (query simple-successful-join cold-windy-query)
           (query complex-successful-join cold-windy-query)
           [{:?t 0 :?w 50}]))

    (is (= (get-condition-match simple-successful-join #?(:clj Temperature :cljs `Temperature))
           (get-condition-match complex-successful-join #?(:clj Temperature :cljs `Temperature))
           [(->Temperature 0 "MCI")]))

    (is (= (get-condition-match simple-failed-join #?(:clj Temperature :cljs `Temperature))
           (get-condition-match complex-failed-join #?(:clj Temperature :cljs `Temperature))
           [(->Temperature 0 "ORD")]))

    (is (= (get-condition-match simple-successful-join #?(:clj WindSpeed :cljs `WindSpeed))
           (get-condition-match complex-successful-join #?(:clj WindSpeed :cljs `WindSpeed))
           (get-condition-match simple-failed-join #?(:clj WindSpeed :cljs `WindSpeed))
           (get-condition-match complex-failed-join #?(:clj WindSpeed :cljs `WindSpeed))
           [(->WindSpeed 50 "MCI")]))))

(tu/def-rules-test test-explain-activations-does-not-crash
  {:rules [cold-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                      (insert! (map->Cold {:temperature :too-cold}))]]
   :sessions [empty-session [cold-rule] {}]}
  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (fire-rules))]
    (is (with-out-str (explain-activations session)))))

(tu/def-rules-test test-unconditional-rule-matches
  
  {:rules [cold-rule [[[Cold]]
                      (insert-unconditional! (->LousyWeather))]]
   :sessions [base-session [cold-rule] {}]}

  (let [fired-no-listening (-> base-session
                               (insert (->Cold 0))
                               fire-rules)
        fired-with-listening (-> base-session
                                 with-full-logging
                                 (insert (->Cold 0))
                                 fire-rules)
        fired-disabled-listening (without-full-logging fired-with-listening)]

    (is (= [(map->Explanation {:matches [{:fact (->Cold 0)
                                          :condition (-> cold-rule :lhs first)}]
                               :bindings {}})]
           (-> fired-with-listening inspect :unfiltered-rule-matches (get cold-rule))))

    (is (= (get (inspect fired-no-listening) :unfiltered-rule-matches ::absent)
           ::absent))
    (is (= (get (inspect fired-disabled-listening) :unfiltered-rule-matches ::absent)
           ::absent))

    ;; No logical insertions happened in any case so we shouldn't have entries in :rule-matches.
    (is (empty?  (-> fired-no-listening inspect :rule-matches (get cold-rule))))
    (is (empty?  (-> fired-with-listening inspect :rule-matches (get cold-rule))))
    (is (empty?  (-> fired-disabled-listening inspect :rule-matches (get cold-rule))))))

