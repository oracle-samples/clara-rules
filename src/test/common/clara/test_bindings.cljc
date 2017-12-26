#?(:clj
   (ns clara.test-bindings
     "Tests focused on the creation of binding variables and their use
      in joins between rule and query condition.  Binding variables
      begin with ?."
     (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert-all
                                    insert!
                                    retract
                                    query]]

               [clara.rules.testfacts :refer [->Temperature ->Cold ->WindSpeed
                                              ->ColdAndWindy]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators :as acc]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature
               Cold
               WindSpeed
               ColdAndWindy]))

   :cljs
   (ns clara.test-bindings
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    insert-all
                                    retract
                                    query]]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->Cold Cold
                                              ->WindSpeed WindSpeed
                                              ->ColdAndWindy ColdAndWindy]]
               [clara.rules.accumulators :as acc]
               [cljs.test]
               [schema.test :as st]
               [clara.tools.testing-utils :as tu])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))
(use-fixtures :each tu/side-effect-holder-fixture)

(def side-effect-atom (atom nil))

(def-rules-test test-multiple-comparison-binding
  {:rules [cold-rule [[[Temperature (= ?t temperature 10)]]
                      (reset! tu/side-effect-holder ?t)]]

   :sessions [empty-session [cold-rule] {}]}

  (-> empty-session
      (insert (->Temperature 10 "MCI"))
      fire-rules)

  (is (= @tu/side-effect-holder 10)))

(def-rules-test test-simple-join-binding
  
  {:rules [same-wind-and-temp [[[Temperature (= ?t temperature)]
                                [WindSpeed (= ?t windspeed)]]
                               (reset! tu/side-effect-holder ?t)]]
   
   :sessions [empty-session [same-wind-and-temp] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10  "MCI"))
                    (insert (->WindSpeed 10  "MCI")))]

    (fire-rules session)
    (is (= 10 @tu/side-effect-holder))))

(def-rules-test test-simple-join-binding-nomatch

  {:rules [same-wind-and-temp [[[Temperature (= ?t temperature)]
                                [WindSpeed (= ?t windspeed)]]
                               (reset! tu/side-effect-holder ?t)]]
   
   :sessions [empty-session [same-wind-and-temp] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10  "MCI"))
                    (insert (->WindSpeed 20  "MCI")))]

    (fire-rules session)
    (is (nil? @tu/side-effect-holder))))

(def-rules-test test-join-with-fact-binding

  {:rules [same-wind-and-temp [[[?t <- Temperature]
                                [?w <- WindSpeed (= ?t windspeed)]]
                               (reset! tu/side-effect-holder ?w)]]

   :sessions [empty-session [same-wind-and-temp] {}]}

  ;; The bound item in windspeed does not match the temperature,
  ;; so this should have no result.
  (-> empty-session
      (insert (->Temperature 10  "MCI"))
      (insert (->WindSpeed (->Temperature 20 "MCI") "MCI"))
      (fire-rules))

  (is (nil? @tu/side-effect-holder))

  (-> empty-session
      (insert (->Temperature 10  "MCI"))
      (insert (->WindSpeed (->Temperature 10 "MCI") "MCI"))
      (fire-rules))

  (is (= (->WindSpeed (->Temperature 10 "MCI") "MCI")
         @tu/side-effect-holder)))

(def-rules-test test-simple-condition-binding

  {:queries [cold-query [[]
                         [[?t <- Temperature (< temperature 20)]]]]

   :sessions [empty-session [cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    fire-rules)]

    (is (= #{{:?t (->Temperature 15 "MCI")}
             {:?t (->Temperature 10 "MCI")}}
           (set (query session cold-query))))))

(def-rules-test test-condition-and-value-binding

  {:queries [cold-query [[]
                         [[?t <- Temperature (< temperature 20) (= ?v temperature)]]]]

   :sessions [empty-session [cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    fire-rules)]

    ;; Ensure the condition's fact and values are all bound.
    (is (= (frequencies [{:?v 10, :?t (->Temperature 10 "MCI")}
                         {:?v 15, :?t (->Temperature 15 "MCI")}])
           (frequencies (query session cold-query))))))

;; Test for https://github.com/cerner/clara-rules/issues/142
(def-rules-test test-beta-binding

  {:queries [beta-bind-query [[]
                              [[Temperature (= ?x temperature)]
                               [ColdAndWindy (= ?t (+ temperature ?x))]]]

             increment-query [[]
                              [[Temperature (= ?x temperature)]
                               [ColdAndWindy (= ?t (inc ?x))]]]]

   :sessions [beta-bind-empty-session [beta-bind-query] {}
              increment-empty-session [increment-query] {}]}

  (is (= [{:?x 10 :?t 15}]
         (-> beta-bind-empty-session
             (insert (->Temperature 10 "MCI")
                     (->ColdAndWindy 5 50))
             (fire-rules)
             fire-rules
             (query beta-bind-query))))

  ;; Test retraction
  (is (empty?
       (-> beta-bind-empty-session
           (insert (->Temperature 10 "MCI")
                   (->ColdAndWindy 5 50))
           (fire-rules)
           (retract (->Temperature 10 "MCI"))
           fire-rules
           (query beta-bind-query))))

  ;; Test version that didn't compile with issue 142.
  (is (= [{:?x 10 :?t 11}]
         (-> increment-empty-session
             (insert (->Temperature 10 "MCI")
                     (->ColdAndWindy 5 50))
             fire-rules
             (query increment-query)))))

(def-rules-test test-non-binding-equality

  {:queries [temps-with-addition [[]
                                  [[Temperature (= ?t1 temperature)
                                    (= "MCI" location )]
                                   [Temperature (= ?t2 temperature)
                                    (= ?foo (+ 20 ?t1))
                                    (= "SFO" location)]
                                   [Temperature (= ?t3 temperature)
                                    (= ?foo (+ 10 ?t2))
                                    (= "ORD" location)]]]

             temps-with-negation [[]
                                  [[Temperature (= ?t1 temperature)
                                    (= "MCI" location )]
                                   [Temperature (= ?t2 temperature)
                                    (= ?foo (+ 20 ?t1))
                                    (= "SFO" location)]
                                   [:not [Temperature (= ?foo (+ 10 ?t2))
                                          (= "ORD" location)]]]]]

   :sessions [empty-session [temps-with-addition temps-with-negation] {}]}

  (let [session (fire-rules empty-session)]

    ;; Test a match.
    (is (= [{:?t3 30, :?t2 20, :?t1 10, :?foo 30}]
           (-> session
               (insert (->Temperature 10 "MCI")
                       (->Temperature 20 "SFO")
                       (->Temperature 30 "ORD"))
               (fire-rules)
               (query temps-with-addition))))

    
    ;; Test if not all conditions are satisfied.
    (is (empty? (-> session
                    (insert (->Temperature 10 "MCI")
                            (->Temperature 21 "SFO")
                            (->Temperature 30 "ORD"))
                    (fire-rules)
                    (query temps-with-addition))))

    ;; Test if there is a negated element.
    (is (empty? (-> session
                    (insert (->Temperature 10 "MCI")
                            (->Temperature 20 "SFO")
                            (->Temperature 30 "ORD"))
                    (fire-rules)
                    (query temps-with-negation))))

    ;; Test there is a match when the negated element does not exist.
    (is (= [{:?t2 20, :?t1 10, :?foo 30}]
           (-> session
               (insert (->Temperature 10 "MCI")
                       (->Temperature 20 "SFO"))
               (fire-rules)
               (query temps-with-negation))))))

;; Test for https://github.com/cerner/clara-rules/issues/267
;; This test has a counterpart of the same name in test-rules for
;; an error-checking case; once we land on a strategy for error-checking
;; test cases in ClojureScript we can move that test case here and eliminate the
;; test there.
(def-rules-test test-local-scope-visible-in-join-filter

  {:queries [check-local-binding [[] [[WindSpeed (= ?w windspeed)]
                                      [Temperature (= ?t temperature)
                                       (tu/join-filter-equals ?w ?t 10)]]]

             check-local-binding-accum [[] [[WindSpeed (= ?w windspeed)]
                                            [?results <- (acc/all) :from [Temperature (= ?t temperature)
                                                                          (tu/join-filter-equals ?w ?t 10)]]]]

             check-reuse-previous-binding [[] [[WindSpeed (= ?t windspeed) (= ?w windspeed)]
                                               [Temperature (= ?t temperature)
                                                (tu/join-filter-equals ?w ?t 10)]]]

             check-accum-result-previous-binding [[]
                                                  [[?t <- (acc/min :temperature) :from [Temperature]]
                                                   [ColdAndWindy (= ?t temperature) (tu/join-filter-equals ?t windspeed)]]]]

   :sessions [check-local-binding-session [check-local-binding] {}
              check-local-binding-accum-session [check-local-binding-accum] {}
              check-reuse-previous-binding-session [check-reuse-previous-binding] {}
              check-accum-result-previous-binding-session [check-accum-result-previous-binding] {}]}

  (is (= [{:?w 10 :?t 10}]
         (-> check-local-binding-session
             (insert (->WindSpeed 10 "MCI") (->Temperature 10 "MCI"))
             (fire-rules)
             (query check-local-binding))))

  (is (= [{:?w 10 :?t 10 :?results [(->Temperature 10 "MCI")]}]
         (-> check-local-binding-accum-session
             (insert (->WindSpeed 10 "MCI") (->Temperature 10 "MCI"))
             (fire-rules)
             (query check-local-binding-accum))))

  (is (= [{:?w 10 :?t 10}]
         (-> check-reuse-previous-binding-session
             (insert (->WindSpeed 10 "MCI") (->Temperature 10 "MCI"))
             (fire-rules)
             (query check-reuse-previous-binding))))

  (is (empty? (-> check-accum-result-previous-binding-session
                  (insert (->Temperature -10 "MCI"))
                  (insert (->ColdAndWindy -20 -20))
                  fire-rules
                  (query check-accum-result-previous-binding)))
      "Validate that the ?t binding from the previous accumulator result is used, rather
         than the binding in the ColdAndWindy condition that would create a ?t binding if one were
         not already present"))
