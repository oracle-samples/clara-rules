#?(:clj
   (ns clara.test-blacklist
     (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    query]]

               [clara.rules.testfacts :refer [->Temperature
                                              ->Cold
                                              ->Hot
                                              ->TemperatureHistory
                                              ->ColdAndWindy]]
               [clojure.test :refer [is]]
               [clara.rules.accumulators])
     (:import [clara.rules.testfacts
               Temperature
               Cold
               Hot
               ColdAndWindy
               TemperatureHistory]))

   :cljs
   (ns clara.test-blacklist
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    query]]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->Cold Cold
                                              ->Hot Hot
                                              ->ColdAndWindy ColdAndWindy
                                              ->TemperatureHistory TemperatureHistory]]
               [clara.rules.accumulators]
               [cljs.test])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is]])))

(def-rules-test test-blacklist-a-rule
  {:rules [cold-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                      (insert! (->Cold ?t))]
           history-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                         (insert! (->TemperatureHistory [?t]))]]

   :queries [cold-query [[] [[Cold (= ?c temperature)]]]
             history-query [[] [[TemperatureHistory (= ?t temperatures)]]]]

   :sessions [empty-session [cold-rule cold-query history-rule history-query] {:blacklist ['cold-rule]}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))]

    (is (empty? (query session cold-query))) ;removed rule

    (is (= #{{:?t [10]}}
           (set (query session history-query))))))

(def-rules-test test-blacklist-many-rules
  {:rules [cold-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                      (insert! (->Cold ?t))]
           history-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                         (insert! (->TemperatureHistory [?t]))]
           hot-rule [[[Temperature (> temperature 50) (= ?t temperature)]]
                     (insert! (->Hot ?t))]
           cold-and-windy-rule [[[Cold (> temperature 30) (= ?t temperature)]]
                                (insert! (->ColdAndWindy ?t 60))]]

   :queries [cold-query [[] [[Cold (= ?c temperature)]]]
             history-query [[] [[TemperatureHistory (= ?ht temperatures)]]]
             hot-query [[] [[Hot (= ?h temperature)]]]
             cold-windy-query [[] [[ColdAndWindy (= ?cw temperature)]]]]

   :sessions [empty-session [cold-rule cold-query
                             history-rule history-query
                             hot-rule hot-query
                             cold-and-windy-rule cold-windy-query] {:blacklist ['history-rule
                                                                                'hot-rule
                                                                                'cold-and-windy-rule]}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Cold 40))
                    (insert (->Temperature 70 "MCI"))
                    (fire-rules))]

    (is (empty? (query session history-query)))
    (is (empty? (query session hot-query)))
    (is (empty? (query session cold-windy-query)))

    (is (= #{{:?c 10} {:?c 40}}
           (set (query session cold-query))))))


(def-rules-test test-blacklist-a-query
  {:rules [cold-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                      (insert! (->Cold ?t))]]

   :queries [cold-query [[] [[Cold (= ?c temperature)]]]]

   :sessions [empty-session [cold-rule cold-query] {:blacklist ['cold-query]}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))]
    (is (thrown? java.lang.IllegalArgumentException
                 (query session cold-query)))))
