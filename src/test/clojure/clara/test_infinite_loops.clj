(ns clara.test-infinite-loops
  (:require [clojure.test :refer :all]
            [clara.rules :refer :all]
            [clara.rules.testfacts :refer [->Cold  ->Hot]]
            [clara.tools.testing-utils :refer [def-rules-test]]
            [clara.test-rules :refer [assert-ex-data]])
  (:import [clara.rules.testfacts Temperature WindSpeed Cold Hot TemperatureHistory
            ColdAndWindy LousyWeather First Second Third Fourth FlexibleFields]))

(def-rules-test test-simple-loop

  {:rules [hot-rule [[[:not [Hot]]]
                     (insert! (->Cold nil))]

           cold-rule [[[Cold]]
                      (insert! (->Hot nil))]]

   :sessions [empty-session [hot-rule cold-rule] {:max-cycles 3000}]}

  (assert-ex-data  {:clara-rules/infinite-loop-suspected true}  (fire-rules empty-session)))


(def-rules-test test-recursive-insertion

  {:rules [cold-rule [[[Cold]]
                      (insert! (->Cold "ORD"))]]

   ;; Use a small value here to ensure that we don't run out of memory before throwing.
   ;; There is unfortunately a tradeoff where making the default number of cycles allowed
   ;; high enough for some use cases will allow others to create OutOfMemory errors in cases
   ;; like these but we can at least throw when there is enough memory available to hold the facts
   ;; created in the loop.
   :sessions [empty-session [cold-rule] {:max-cycles 10}]}

  (assert-ex-data {:clara-rules/infinite-loop-suspected true}
                  (-> empty-session
                      (insert (->Cold "ARN"))
                      fire-rules)))



