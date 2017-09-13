#?(:clj
   (ns clara.test-memory
     (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert-all
                                    insert!
                                    retract
                                    query]]
               [clara.rules.testfacts :refer [->Temperature ->Cold ->WindSpeed ->Hot
                                              ->ColdAndWindy ->First ->Second]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators :as acc]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature
               Hot
               Cold
               WindSpeed
               ColdAndWindy
               First
               Second]))

   :cljs
   (ns clara.test-memory
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    insert-all
                                    retract
                                    query]]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->Cold Cold
                                              ->Hot Hot
                                              ->WindSpeed WindSpeed
                                              ->ColdAndWindy ColdAndWindy
                                              ->First First
                                              ->Second Second]]
               [clara.rules.accumulators :as acc]
               [clara.tools.testing-utils :as tu]
               [cljs.test]
               [schema.test :as st])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))

;; While the memory is tested through rules and queries, rather than direct unit tests on the memory,
;; the intent of these tests is to create patterns in the engine that cover edge cases and other paths
;; of concern in clara.rules.memory.
;; This test are here to verify https://github.com/cerner/clara-rules/issues/303
(def-rules-test test-negation-complex-join-with-numerous-non-matching-facts-inserted-after-descendant-negation
  {:rules []
   :queries [query1 [[]
                     [[Hot (= ?t temperature)]
                      [:not [Cold (tu/join-filter-equals ?t temperature)]]]]
             query2 [[]
                     [[Hot (= ?t temperature)]
                      [:not [Cold (= ?t temperature)]]]]]
   :sessions [empty-session-jfe [query1] {}
              empty-session-equals [query2] {}]}
  (let [lots-of-hot (doall (for [_ (range 100)]
                             (->Hot 20)))]
    (is (= (repeat 100 {:?t 20})
           (-> empty-session-jfe
               (insert (->Cold 10))
               fire-rules
               (insert-all lots-of-hot)
               fire-rules
               (query query1))))

    (is (= (repeat 100 {:?t 20})
           (-> empty-session-equals
               (insert (->Cold 10))
               fire-rules
               (insert-all lots-of-hot)
               fire-rules
               (query query2))))))

(def-rules-test test-query-for-many-added-elements
  {:queries [temp-query [[] [[Temperature (= ?t temperature)]]]]

   :sessions [empty-session [temp-query] {}]}

  (let [n 6000
        ;; Do not batch insert to expose any StackOverflowError potential
        ;; of stacking lazy evaluations in working memory.
        session (reduce insert empty-session
                        (for [i (range n)] (->Temperature i "MCI")))
        session (fire-rules session)]

    (is (= n
           (count (query session temp-query))))))

(def-rules-test test-query-for-many-added-tokens

  {:rules [cold-temp [[[Temperature (< temperature 30) (= ?t temperature)]]
                      (insert! (->Cold ?t))]]

   :queries [cold-query [[] [[Cold (= ?t temperature)]]]]

   :sessions [empty-session [cold-temp cold-query] {}]}

  (let [n 6000

        ;; Do not batch insert to expose any StackOverflowError potential
        ;; of stacking lazy evaluations in working memory.
        session (reduce insert empty-session
                        (for [i (range n)] (->Temperature (- i) "MCI")))

        session (fire-rules session)]

    (is (= n
           (count (query session cold-query))))))


(def-rules-test test-many-retract-accumulated-for-same-accumulate-with-join-filter-node

  {:rules [count-cold-temps [[[Cold (= ?cold-temp temperature)]
                              [?temp-count <- (acc/count) :from [Temperature (some? temperature) (<= temperature ?cold-temp)]]]
                             (insert! {:count ?temp-count
                                       :type :temp-counter})]]

   :queries [cold-temp-count-query [[] [[:temp-counter [{:keys [count]}] (= ?count count)]]]]

   :sessions [empty-session [count-cold-temps cold-temp-count-query] {:fact-type-fn (fn [f]
                                                                                      (or (:type f)
                                                                                          (type f)))}]}

  (let [n 6000

        session  (reduce insert empty-session
                         ;; Insert all temperatures one at a time to ensure the
                         ;; accumulate node will continuously re-accumulate via
                         ;; `right-activate-reduced` to expose any StackOverflowError
                         ;; potential of stacking lazy evaluations in working memory.
                         (for [t (range n)] (->Temperature (- t) "MCI")))
        session (-> session
                    (insert (->Cold 30))
                    fire-rules)]

    ;; All temperatures are under the Cold temperature threshold.
    (is (= #{{:?count 6000}} (set (query session cold-temp-count-query))))))

(def-rules-test test-disjunctions-sharing-production-node
  ;; Ensures that when 'sibling' nodes are sharing a common child
  ;; production node, that activations are effectively retracted in some
  ;; TMS control flows.
  ;; See https://github.com/cerner/clara-rules/pull/145 for more context.

  {:rules [r [[[:or
                [First]
                [Second]]
               [?ts <- (acc/all) :from [Temperature]]]
              (insert! (with-meta {:ts ?ts}
                         {:type :holder}))]]

   :queries [q [[]
                [[?h <- :holder]]]]

   :sessions [s [r q] {}]}

  (let [;; Vary the insertion order to ensure that the outcomes are the same.
        ;; This insertion order will cause retractions to need to be propagated
        ;; to the RHS production node that is shared by the nested conditions
        ;; of the disjunction.
        qres1 (-> s
                  (insert (->First))
                  (insert (->Temperature 1 "MCI"))
                  (insert (->Second))
                  (insert (->Temperature 2 "MCI"))
                  fire-rules
                  (query q)
                  set)
        qres2 (-> s
                  (insert (->First))
                  (insert (->Temperature 1 "MCI"))
                  (insert (->Temperature 2 "MCI"))
                  (insert (->Second))
                  fire-rules
                  (query q)
                  set)]
    (is (= qres1 qres2))))

(def-rules-test test-force-multiple-transient-transitions-activation-memory
  ;; The objective of this test is to verify that activation memory works
  ;; properly after going through persistent/transient shifts, including shifts
  ;; that empty it (i.e. firing the rules.)

  {:rules [rule [[[ColdAndWindy]]
                 (insert! (->Cold 10))]]

   :queries [cold-query [[]
                         [[Cold (= ?t temperature)]]]]

   :sessions [empty-session [rule cold-query] {}]}

  (let [windy-fact (->ColdAndWindy 20 20)]

    (is (= (-> empty-session
               (insert windy-fact)
               (insert windy-fact)
               fire-rules
               (query cold-query))
           [{:?t 10} {:?t 10}])
        "Make two insert calls forcing the memory to go through a persistent/transient transition
         in between insert calls.")

    (is (= (-> empty-session
               (insert windy-fact)
               (insert windy-fact)
               fire-rules
               (insert windy-fact)
               (insert windy-fact)
               fire-rules
               (query cold-query))
           (repeat 4 {:?t 10}))
        "Validate that we can still go through a persistent/transient transition in the memory
         after firing rules causes the activation memory to be emptied.")))
