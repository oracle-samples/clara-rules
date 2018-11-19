#?(:clj
   (ns clara.test-exists
     (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert-all
                                    insert-unconditional!
                                    insert!
                                    retract
                                    query]]

               [clara.rules.testfacts :refer [->Temperature ->Cold ->WindSpeed
                                              ->ColdAndWindy]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature
               Cold
               WindSpeed
               ColdAndWindy]))

   :cljs
   (ns clara.test-exists
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    insert-all
                                    insert-unconditional!
                                    retract
                                    query]]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->Cold Cold
                                              ->WindSpeed WindSpeed
                                              ->ColdAndWindy ColdAndWindy]]
               [clara.rules.accumulators]
               [cljs.test]
               [schema.test :as st])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))

(def-rules-test test-simple-exists
  {:queries [has-windspeed [[] [[:exists [WindSpeed (= ?location location)]]]]]

   :sessions [empty-session [has-windspeed] {}]}

  ;; An empty session should produce no results.
  (is (empty? (query empty-session has-windspeed)))

  ;; Should only match one windspeed despite multiple inserts.
  (is (= [{:?location "MCI"}]
         (-> empty-session
             (insert (->WindSpeed 50 "MCI"))
             (insert (->WindSpeed 60 "MCI"))
             fire-rules
             (query has-windspeed))))

  ;; Retraction should remove exists check.
  (is (empty?
       (-> empty-session
           (insert (->WindSpeed 50 "MCI"))
           (insert (->WindSpeed 60 "MCI"))
           (retract (->WindSpeed 50 "MCI"))
           (retract (->WindSpeed 60 "MCI"))
           fire-rules
           (query has-windspeed))))

  ;; There should be one location for each distinct binding.
  (is (= #{{:?location "MCI"} {:?location "SFO"} {:?location "ORD"}}
         (-> empty-session
             (insert (->WindSpeed 50 "MCI"))
             (insert (->WindSpeed 60 "MCI"))
             (insert (->WindSpeed 60 "SFO"))
             (insert (->WindSpeed 80 "SFO"))
             (insert (->WindSpeed 80 "ORD"))
             (insert (->WindSpeed 90 "ORD"))
             fire-rules
             (query has-windspeed)
             (set)))))

(def-rules-test test-exists-with-conjunction
  {:queries [wind-and-temp [[] [[:exists [Temperature (= ?location location)]]
                                [:exists [WindSpeed (= ?location location)]]]]]

   :sessions [empty-session [wind-and-temp] {}]}

  ;; An empty session should produce no results.
  (is (empty? (query empty-session wind-and-temp)))

  ;; A match of one exist but not the other should yield nothing.
  (is (empty?
       (-> empty-session
           (insert (->WindSpeed 50 "MCI"))
           (insert (->WindSpeed 60 "MCI"))
           fire-rules
           (query wind-and-temp))))

  ;; Differing locations should not yield a match.
  (is (empty?
       (-> empty-session
           (insert (->WindSpeed 50 "MCI"))
           (insert (->Temperature 60 "ORD"))
           fire-rules
           (query wind-and-temp))))

  ;; Simple match for both exists.
  (is (= [{:?location "MCI"}]
         (-> empty-session
             (insert (->WindSpeed 50 "MCI"))
             (insert (->WindSpeed 60 "MCI"))
             (insert (->Temperature 60 "MCI"))
             (insert (->Temperature 70 "MCI"))
             fire-rules
             (query wind-and-temp))))

  ;; There should be a match for each distinct city.
  (is (= #{{:?location "MCI"} {:?location "ORD"}}
         (-> empty-session
             (insert (->WindSpeed 50 "MCI"))
             (insert (->WindSpeed 60 "ORD"))
             (insert (->Temperature 60 "MCI"))
             (insert (->Temperature 70 "ORD"))
             fire-rules
             (query wind-and-temp)
             (set)))))

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

;; Test of the performance optimization in https://github.com/cerner/clara-rules/issues/298
;; The idea is that if inserting additional items beyond the first causes a retraction and then
;; rebuilding of the Rete network an unconditional insertion will happen twice.
(def-rules-test test-additional-item-noop

  {:rules [exists-rule [[[:exists [Temperature (< temperature 0)]]]
                         (insert-unconditional! (->Cold :freezing))]]

   :queries [cold-query [[] [[Cold (= ?t temperature)]]]]

   :sessions [empty-session [exists-rule cold-query] {}]}

  (is (= [{:?t :freezing}]
         (-> empty-session
             (insert (->Temperature -10 "INV"))
             (fire-rules)
             (insert (->Temperature -10 "INV"))
             fire-rules
             (query cold-query)))))
