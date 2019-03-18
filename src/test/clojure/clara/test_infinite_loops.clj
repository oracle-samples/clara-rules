(ns clara.test-infinite-loops
  (:require [clojure.test :refer :all]
            [clara.rules :refer :all]
            [clara.rules.testfacts :refer [->Cold  ->Hot]]
            [clara.tools.testing-utils :refer [def-rules-test
                                               ex-data-maps]]
            [clara.test-rules :refer [assert-ex-data]]
            [clara.tools.tracing :as tr])
  (:import [clara.rules.testfacts Temperature WindSpeed Cold Hot TemperatureHistory
            ColdAndWindy LousyWeather First Second Third Fourth FlexibleFields]
           [clara.tools.tracing
            PersistentTracingListener]))

(def-rules-test test-simple-loop

  {:rules [hot-rule [[[:not [Hot]]]
                     (insert! (->Cold nil))]

           cold-rule [[[Cold]]
                      (insert! (->Hot nil))]]

   :sessions [empty-session [hot-rule cold-rule] {}]}

  (assert-ex-data  {:clara-rules/infinite-loop-suspected true}  (fire-rules empty-session {:max-cycles 3000})))


(def-rules-test test-recursive-insertion

  {:rules [cold-rule [[[Cold]]
                      (insert! (->Cold "ORD"))]]

   :sessions [empty-session [cold-rule] {}]}

  (assert-ex-data {:clara-rules/infinite-loop-suspected true}
                  (-> empty-session
                      (insert (->Cold "ARN"))
                      ;; Use a small value here to ensure that we don't run out of memory before throwing.
                      ;; There is unfortunately a tradeoff where making the default number of cycles allowed
                      ;; high enough for some use cases will allow others to create OutOfMemory errors in cases
                      ;; like these but we can at least throw when there is enough memory available to hold the facts
                      ;; created in the loop.
                      (fire-rules {:max-cycles 10}))))

(def-rules-test test-tracing-infinite-loop

  {:rules [cold-rule [[[Cold]]
                      (insert! (->Cold "ORD"))]]

   :sessions [empty-session [cold-rule] {}]}

  (try
    (do (-> empty-session
            tr/with-tracing
            (insert (->Cold "ARN"))
            ;; Use a small value here to ensure that we don't run out of memory before throwing.
            ;; There is unfortunately a tradeoff where making the default number of cycles allowed
            ;; high enough for some use cases will allow others to create OutOfMemory errors in cases
            ;; like these but we can at least throw when there is enough memory available to hold the facts
            ;; created in the loop.
            (fire-rules {:max-cycles 10}))
        (is false "The infinite loop did not throw an exception."))
    (catch Exception e
      (let [data-maps (ex-data-maps e)
            loop-data (filter :clara-rules/infinite-loop-suspected data-maps)]
        (is (= (count loop-data)
               1)
            "There should only be one exception in the chain from infinite rules loops.")
        (is (= (-> loop-data first  :listeners count) 1)
            "There should only be one listener.")
        (is (-> loop-data first  :listeners ^PersistentTracingListener first .-trace not-empty)
            "There should be tracing data available when a traced session throws an exception on an infinite loop.")))))
            
  





