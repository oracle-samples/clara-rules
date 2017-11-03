#?(:clj
   (ns clara.test-rhs-retract
     (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert-all
                                    insert!
                                    retract
                                    query
                                    retract!]]

               [clara.rules.testfacts :refer [->Temperature ->Cold]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature
               Cold]))

   :cljs
   (ns clara.test-rhs-retract
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    insert-all
                                    retract
                                    query
                                    retract!]]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->Cold Cold]]
               [clara.rules.accumulators]
               [cljs.test]
               [schema.test :as st])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))

(def-rules-test test-retract!

  {:rules [not-cold-rule [[[Temperature (> temperature 50)]]
                          (retract! (->Cold 20))]]

   :queries [cold-query [[]
                         [[Cold (= ?t temperature)]]]]

   :sessions [empty-session [not-cold-rule cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Cold 20))
                    (fire-rules))]
    
    ;; The session should contain our initial cold reading.
    (is (= #{{:?t 20}}
           (set (query session cold-query))))

    ;; Insert a higher temperature and ensure the cold fact was retracted.
    (is (= #{}
           (set (query (-> session
                           (insert (->Temperature 80 "MCI"))
                           (fire-rules))
                       cold-query))))))
