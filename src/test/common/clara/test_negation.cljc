#?(:clj
   (ns clara.test-negation
     (:require [clara.tools.testing-utils :refer [def-rules-test
                                                  side-effect-holder] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert-all
                                    insert-all!
                                    insert!
                                    retract
                                    query]]

               [clara.rules.testfacts :refer [->Temperature ->Cold ->WindSpeed
                                              ->ColdAndWindy ->LousyWeather]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature
               Cold
               WindSpeed
               ColdAndWindy
               LousyWeather]))

   :cljs
   (ns clara.test-negation
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    insert-all
                                    insert-all!
                                    retract
                                    query]]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->Cold Cold
                                              ->WindSpeed WindSpeed
                                              ->ColdAndWindy ColdAndWindy
                                              ->LousyWeather LousyWeather]]
               [clara.rules.accumulators]
               [cljs.test]
               [schema.test :as st]
               [clara.tools.testing-utils :refer [side-effect-holder] :as tu])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))

(def-rules-test test-exists-inside-boolean-conjunction-and-disjunction

  {:rules [or-rule [[[:or
                      [:exists [ColdAndWindy]]
                      [:exists [Temperature (< temperature 20)]]]]
                    (insert! (->Cold nil))]

           and-rule [[[:and
                       [:exists [ColdAndWindy]]
                       [:exists [Temperature (< temperature 20)]]]]
                     (insert! (->Cold nil))]]

   :queries [cold-query [[] [[Cold (= ?t temperature)]]]]

   :sessions [or-session [or-rule cold-query] {}
              and-session [and-rule cold-query] {}]}

  (is (empty? (-> or-session
                  fire-rules
                  (query cold-query)))
      "Verify that :exists under an :or does not fire if nothing meeting the :exists is present")

  (is (= (-> or-session
             (insert (->ColdAndWindy 10 10))
             fire-rules
             (query cold-query))
         [{:?t nil}])
      "Validate that :exists can match under a boolean :or condition.")

  (is (empty? (-> and-session
                  (insert (->ColdAndWindy 10 10))
                  fire-rules
                  (query cold-query)))
      "Validate that :exists under an :and condition without both conditions does not cause the rule to fire.")

  (is (= (-> and-session
             (insert (->ColdAndWindy 10 10) (->Temperature 10 "MCI"))
             fire-rules
             (query cold-query))
         [{:?t nil}])
      "Validate that :exists can match under a boolean :and condition."))

(def-rules-test test-simple-negation

  {:queries [not-cold-query [[] [[:not [Temperature (< temperature 20)]]]]]

   :sessions [empty-session [not-cold-query] {}]}

  (let [session-with-temp (-> empty-session
                              (insert (->Temperature 10 "MCI"))
                              fire-rules)
        session-retracted (-> session-with-temp
                              (retract (->Temperature 10 "MCI"))
                              fire-rules)
        session-with-partial-retraction (-> empty-session
                                            (insert (->Temperature 10 "MCI")
                                                    (->Temperature 15 "MCI"))
                                            (retract (->Temperature 10 "MCI"))
                                            fire-rules)]

    ;; No facts for the above criteria exist, so we should see a positive result
    ;; with no bindings.
    (is (= #{{}}
           (set (query empty-session not-cold-query))))

    ;; Inserting an item into the sesion should invalidate the negation.
    (is (empty? (query session-with-temp
                       not-cold-query)))

    ;; Retracting the inserted item should make the negation valid again.
    (is (= #{{}}
           (set (query session-retracted not-cold-query))))

    ;; Some but not all items were retracted, so the negation
    ;; should still block propagation.
    (is (empty? (query session-with-partial-retraction
                       not-cold-query)))))

(def-rules-test test-negation-truth-maintenance

  {:rules [make-hot [[[WindSpeed]] ; Hack to only insert item on rule activation.
                     (insert! (->Temperature 100 "MCI"))]

           not-hot [[[:not [Temperature (> temperature 80)]]]
                    (insert! (->Cold 0))]]

   :queries [cold-query [[] [[?c <- Cold]]]]

   :sessions [empty-session  [make-hot not-hot cold-query] {}]}

  (let [base-session (fire-rules empty-session)]

  ;; The cold fact should exist because nothing matched the negation.
    (is (= [{:?c (->Cold 0)}] (query base-session cold-query)))

  ;; The cold fact should be retracted because inserting this
  ;; triggers an insert that matches the negation.
  (is (empty? (-> empty-session
                  (insert (->WindSpeed 100 "MCI"))
                  (fire-rules)
                  (query cold-query))))

  ;; The cold fact should exist again because we are indirectly retracting
  ;; the fact that matched the negation originially
  (is (= [{:?c (->Cold 0)}]
         (-> empty-session
             (insert (->WindSpeed 100 "MCI"))
             (fire-rules)
             (retract (->WindSpeed 100 "MCI"))
             (fire-rules)
             (query cold-query))))))

(def-rules-test test-negation-with-other-conditions

  {:queries [windy-but-not-cold-query [[] [[WindSpeed (> windspeed 30) (= ?w windspeed)]
                                           [:not [ Temperature (< temperature 20)]]]]]

   :sessions [empty-session [windy-but-not-cold-query] {}]}

  (let [;; Make it windy, so our query should indicate that.
        session (-> empty-session
                    (insert (->WindSpeed 40 "MCI"))
                    fire-rules)
        windy-result  (set (query session windy-but-not-cold-query))

        ;; Make it hot and windy, so our query should still succeed.
        session (-> session
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)
        hot-and-windy-result (set (query session windy-but-not-cold-query))

        ;; Make it cold, so our query should return nothing.
        session (-> session
                    (insert (->Temperature 10 "MCI"))
                    fire-rules)
        cold-result  (set (query session windy-but-not-cold-query))]

    (is (= #{{:?w 40}} windy-result))
    (is (= #{{:?w 40}} hot-and-windy-result))

    (is (empty? cold-result))))

(def-rules-test test-negated-conjunction

  {:queries [not-cold-and-windy [[] [[:not [:and
                                            [WindSpeed (> windspeed 30)]
                                            [Temperature (< temperature 20)]]]]]]

   :sessions [empty-session [not-cold-and-windy] {}]}

  (let [session-with-data (-> empty-session
                              (insert (->WindSpeed 40 "MCI"))
                              (insert (->Temperature 10 "MCI"))
                              (fire-rules))]

    ;; It is not cold and windy, so we should have a match.
    (is (= #{{}}
           (set (query empty-session not-cold-and-windy))))

    ;; Make it cold and windy, so there should be no match.
    (is (empty? (query session-with-data not-cold-and-windy)))))

(def-rules-test test-negated-disjunction

  {:queries [not-cold-or-windy [[] [[:not [:or [WindSpeed (> windspeed 30)]
                                           [Temperature (< temperature 20)]]]]]]

   :sessions [empty-session [not-cold-or-windy] {}]}

  (let [session-with-temp (fire-rules (insert empty-session (->WindSpeed 40 "MCI")))
        session-retracted (fire-rules (retract session-with-temp (->WindSpeed 40 "MCI")))]

    ;; It is not cold and windy, so we should have a match.
    (is (= #{{}}
           (set (query empty-session not-cold-or-windy))))

    ;; Make it cold and windy, so there should be no match.
    (is (empty? (query session-with-temp not-cold-or-windy)))

    ;; Retract the added fact and ensure we now match something.
    (is (= #{{}}
           (set (query session-retracted not-cold-or-windy))))))

(def-rules-test test-complex-negation

  {:queries [cold-not-match-temp [[]
                                  [[:not [:and
                                          [?t <- Temperature]
                                          [Cold (= temperature (:temperature ?t))]]]]]

             negation-with-prior-bindings [[]
                                           [[WindSpeed (= ?l location)]
                                            [:not [:and
                                                   [?t <- Temperature (= ?l location)]
                                                   [Cold (= temperature (:temperature ?t))]]]]]

             nested-negation-with-prior-bindings [[]
                                                  [[WindSpeed (= ?l location)]
                                                   [:not [:and
                                                          [?t <- Temperature (= ?l location)]
                                                          [:not [Cold (= temperature (:temperature ?t))]]]]]]

             ;; The idea here is to return all facts in the session from the user's point of view.  The user
             ;; should not be exposed to Clara's internal facts, such as complex negation result markers, through query results.
             ;; On the JVM we could simply use java.lang.Object but this approach has the advantage of being cross-platform.  Note
             ;; the :ancestors-fn specified in the session definitions below that adds ::any as an ancestor of all facts in the session.
             object-query [[] [[?o <- ::any]]]]

   :sessions [s [cold-not-match-temp object-query] {:ancestors-fn (fn [t]
                                                                    (conj (ancestors t) ::any))}
              s-with-prior [negation-with-prior-bindings object-query] {:ancestors-fn (fn [t]
                                                                                        (conj (ancestors t) ::any))}
              s-with-nested [nested-negation-with-prior-bindings object-query] {:ancestors-fn (fn [t]
                                                                                                (conj (ancestors t) ::any))}]}

  (let [no-system-types? (fn [session facts-expected?]
                           (is (or
                                (not facts-expected?)
                                (not-empty (query session object-query)))
                               "If the object query returns nothing the setup of this test is invalid")
                           (not-any? (fn [fact] #?(:clj (instance? clara.rules.engine.ISystemFact fact)
                                                   :cljs  (isa? :clara.rules.engine/system-type fact)))
                                     (as-> session x
                                       (query x object-query)
                                       (map :?o x))))]

    (testing "Should not match when negation is met."
      (let [end-session (-> s
                            (insert (->Temperature 10 "MCI")
                                    (->Cold 10))
                            (fire-rules))]
        (is (empty? (query end-session cold-not-match-temp)))
        (is (no-system-types? end-session true))))

    (testing "Should have result if only a single item matched."
      (let [end-session (-> s
                            (insert (->Temperature 10 "MCI"))
                            (fire-rules))]
        (is (= [{}]
               (query end-session cold-not-match-temp)))
        (is (no-system-types? end-session true))))

    (testing "Test previous binding is visible."
      (let [end-session (fire-rules s-with-prior)]
        (is (empty? (query end-session negation-with-prior-bindings)))
        (is (no-system-types? end-session false))))

    (testing "Should have result since negation does not match."
      (let [end-session (-> s-with-prior
                            (insert (->WindSpeed 10 "MCI")
                                    (->Temperature 10 "ORD")
                                    (->Cold 10))
                            (fire-rules))]
        (is (= [{:?l "MCI"}]
               (query end-session negation-with-prior-bindings)))
        (is (no-system-types? end-session true))))

    (testing "No result because negation matches."
      (let [end-session (-> s-with-prior
                            (insert (->WindSpeed 10 "MCI")
                                    (->Temperature 10 "MCI")
                                    (->Cold 10))
                            (fire-rules))]
        (is (empty? (query end-session negation-with-prior-bindings)))
        (is (no-system-types? end-session true))))

    (testing "Has nothing because the cold does not match the nested negation,
      so the :and is true and is negated at the top level."
      (let [end-session (-> s-with-nested
                            (insert (->WindSpeed 10 "MCI")
                                    (->Temperature 10 "MCI")
                                    (->Cold 20))
                            (fire-rules))]
        (is (empty?
             (query end-session nested-negation-with-prior-bindings)))
        (is (no-system-types? end-session true))))

    (testing "Match the nested negation, which is then negated again at the higher level,
    so this rule matches."
      (let [end-session (-> s-with-nested
                            (insert (->WindSpeed 10 "MCI")
                                    (->Temperature 10 "MCI")
                                    (->Cold 10))
                            (fire-rules))]
        (is (= [{:?l "MCI"}]
               (query end-session nested-negation-with-prior-bindings)))
        (is (no-system-types? end-session true))))

    (testing "Match the nested negation for location ORD but not for MCI.
    See https://github.com/cerner/clara-rules/issues/304"
      (let [end-session (-> s-with-nested
                            (insert
                             (->WindSpeed 10 "MCI")
                             (->Temperature 10 "MCI")
                             (->Cold 20)
                             (->WindSpeed 20 "ORD")
                             (->Temperature 20 "ORD"))
                            (fire-rules))]

        (is (= [{:?l "ORD"}]
               (query end-session nested-negation-with-prior-bindings)))
        (is (no-system-types? end-session true))))))

(def-rules-test test-complex-negation-custom-type

  {:queries [cold-not-match-temp [[]
                                  [[:not [:and
                                          [?t <- :temperature]
                                          [:cold [{temperature :temperature}] (= temperature (:temperature ?t))]]]]]]

   :sessions [empty-session [cold-not-match-temp] {:fact-type-fn :type}]}

  (is (= [{}]
         (-> empty-session
             (fire-rules)
             (query cold-not-match-temp))))

  ;; Should not match when negation is met.
  (is (empty? (-> empty-session
                  (insert {:type :temperature :temperature 10}
                          {:type :cold :temperature 10})
                  (fire-rules)
                  (query cold-not-match-temp))))

  ;; Should have result if only a single item matched.
  (is (= [{}]
         (-> empty-session
             (insert {:type :temperature :temperature 10})
             (fire-rules)
             (query cold-not-match-temp)))))
