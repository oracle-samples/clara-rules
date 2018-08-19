#?(:clj
   (ns clara.test-testing-utils
     (:require [clara.tools.testing-utils :refer [def-rules-test
                                                  run-performance-test]]
               [clara.rules :as r]

               [clara.rules.testfacts :refer [->Temperature ->Cold]]
               [clojure.test :refer [is deftest run-tests] :as t])
     (:import [clara.rules.testfacts
               Temperature
               Cold]))

   :cljs
   (ns clara.test-testing-utils
     (:require [clara.rules :as r]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->Cold Cold]]
               [cljs.test :as t]
               [clara.tools.testing-utils :refer [run-performance-test]])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer (is deftest run-tests)])))

(def test-ran-atom (atom false))

;; This test fixture validates that def-rules-test actually executed the test bodies it
;; is provided.  If the test bodies were not executed test-ran-atom would have a value of false
;; after test execution.
(t/use-fixtures :once (fn [t]
                        (reset! test-ran-atom false)
                        (t)
                        (is (true? @test-ran-atom))))

(def-rules-test basic-tests
  {:rules [rule1 [[[?t <- Temperature (< temperature 0)]]
                  (r/insert! (->Cold (:temperature ?t)))]]

   :queries [query1 [[]
                     [[Cold (= ?t temperature)]]]]

   :sessions [session1 [rule1 query1] {}
              session2 [rule1 query1] {:fact-type-fn (fn [fact] :bogus)}]}

  (reset! test-ran-atom true)
  (is (= [{:?t -50}]
         (-> session1
             (r/insert (->Temperature -50 "MCI"))
             r/fire-rules
             (r/query query1))))

  ;; Since we validate later (outside the scope of this test) that the state
  ;; change occurred put it in the middle so that it would fail if we took either
  ;; the first or last test form, rather than all test forms.
  (reset! test-ran-atom true)

  (is (empty? (-> session2
                  (r/insert (->Temperature -50 "MCI"))
                  r/fire-rules
                  (r/query query1)))))

(def fire-rules-counter (atom 0))

(def-rules-test test-performance-test
  {:rules [rule1 [[[?t <- Temperature (< temperature 0)]]
                  (swap! fire-rules-counter inc)]]
   :queries []
   :sessions [session1 [rule1] {}]}
  (run-performance-test {:description "Simple fire-rules demonstration"
                         :func #(-> session1
                                    (r/insert (->Temperature -50 "MCI"))
                                    r/fire-rules)
                         :iterations 5
                         :mean-assertion (partial > 500)})
  (is (= @fire-rules-counter 5)))
