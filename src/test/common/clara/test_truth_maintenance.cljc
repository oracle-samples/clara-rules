#?(:clj
   (ns clara.test-truth-maintenance
     (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert-all
                                    insert!
                                    insert-unconditional!
                                    insert-all-unconditional!
                                    retract
                                    retract!
                                    query]]
               [clara.rules.testfacts :refer [->Temperature ->Cold ->WindSpeed
                                              ->TemperatureHistory ->LousyWeather
                                              ->ColdAndWindy ->First ->Second ->Third]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators :as acc]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature
               TemperatureHistory
               Cold
               ColdAndWindy
               WindSpeed
               LousyWeather
               First
               Second
               Third]))

   :cljs
   (ns clara.test-truth-maintenance
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    insert-unconditional!
                                    insert-all-unconditional!
                                    insert-all
                                    retract
                                    retract!
                                    query]]
               [clara.rules.accumulators :as acc]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->TemperatureHistory TemperatureHistory
                                              ->Cold Cold
                                              ->ColdAndWindy ColdAndWindy
                                              ->WindSpeed WindSpeed
                                              ->LousyWeather LousyWeather
                                              ->First First
                                              ->Second Second
                                              ->Third Third]]
               [schema.test :as st])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))

;; Any tests using this atom are expected to reset its value during initialization
;; of the test rather than requiring tests to reset after completing.
(def side-effect-atom (atom nil))

(def-rules-test test-cancelled-activation
  {:rules [cold-rule [[[Temperature (< temperature 20)]]
                      (reset! side-effect-atom ?__token__)]]

   :sessions [empty-session [cold-rule] {}]}

  (reset! side-effect-atom nil)
  (-> empty-session
      (insert (->Temperature 10 "MCI"))
      (retract (->Temperature 10 "MCI"))
      (fire-rules))
  (is (nil? @side-effect-atom)
      "The rule should not fire at all, even with a retraction later, if there is no net fact insertion")

  (reset! side-effect-atom nil)
  (-> empty-session
      (insert (->Temperature 10 "MCI"))
      fire-rules)
  (is (some? @side-effect-atom)
      "Sanity check that the rule fires without a retraction."))

(def-rules-test test-retraction-of-equal-elements
  {:rules [insert-cold [[[Temperature (= ?temp temperature)]]

                        ;; Insert 2 colds that have equal
                        ;; values to ensure they are both
                        ;;retracted
                        (insert! (->Cold ?temp)
                                 (->Cold ?temp))]]

   :queries [find-cold [[] [[?c <- Cold]]]]

   :sessions [empty-session [insert-cold find-cold] {}]}

  (let [;; Each temp should insert 2 colds.
        session-inserted (-> empty-session
                             (insert (->Temperature 50 "LAX"))
                             (insert (->Temperature 50 "MCI"))
                             fire-rules)

        ;; Retracting one temp should retract both of its
        ;; logically inserted colds, but leave the others, even though
        ;; they are equal.
        session-retracted (-> session-inserted
                              (retract (->Temperature 50 "MCI"))
                              fire-rules)]

    (is (= 4 (count (query session-inserted find-cold))))

    (is (= [{:?c (->Cold 50)}
            {:?c (->Cold 50)}
            {:?c (->Cold 50)}
            {:?c (->Cold 50)}]

           (query session-inserted find-cold)))

    (is (= 2 (count (query session-retracted find-cold))))

    (is (= [{:?c (->Cold 50)}
            {:?c (->Cold 50)}]

           (query session-retracted find-cold)))))

(def-rules-test test-insert-and-retract
  {:rules [cold-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                      (insert! (->Cold ?t))]]

   :queries [cold-query [[] [[Cold (= ?c temperature)]]]]

   :sessions [empty-session [cold-rule cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))

        retracted (-> session
                      (retract (->Temperature 10 "MCI"))
                      (fire-rules))]

    (is (= {{:?c 10} 1}
           (frequencies (query session cold-query))))

    ;; Ensure retracting the temperature also removes the logically inserted fact.
    (is (empty?
         (query
          retracted
          cold-query)))))

(def-rules-test test-insert-retract-custom-type

  {:rules [cold-rule [[[:temperature [{value :value}] (< value 20) (= ?t value)]]
                      (insert! {:type :cold :value ?t})]]

   :queries [cold-query [[] [[:cold [{value :value}] (= ?c value)]]]]

   :sessions [empty-session [cold-rule cold-query] {:fact-type-fn :type}]}

  (let [session (-> empty-session
                    (insert {:type :temperature :value 10})
                    (fire-rules))

        retracted (-> session
                      (retract {:type :temperature :value 10})
                      (fire-rules))]

    (is (= {{:?c 10} 1}
           (frequencies (query session cold-query))))

    ;; Ensure retracting the temperature also removes the logically inserted fact.
    (is (empty?
         (query
          retracted
          cold-query)))))

;; Test for issue 67
(def-rules-test test-insert-retract-negation-join

  {:queries [cold-not-windy-query [[] [[Temperature (< temperature 20) (= ?t temperature)]
                                       [:not [WindSpeed]]]]]

   :sessions [empty-session [cold-not-windy-query] {}]}

  (let [session (-> empty-session
                    (insert (->WindSpeed 30 "MCI"))
                    (retract (->WindSpeed 30 "MCI"))
                    (fire-rules))]

    (is (= [{:?t 10}]
           (-> session
               (insert (->Temperature 10 "MCI"))
               (fire-rules)
               (query cold-not-windy-query))))))

(def-rules-test test-unconditional-insert

  {:rules [cold-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                      (insert-unconditional! (->Cold ?t))]]

   :queries [cold-query [[] [[Cold (= ?c temperature)]]]]

   :sessions [empty-session [cold-rule cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))

        retracted-session (-> session
                              (retract (->Temperature 10 "MCI"))
                              fire-rules)]

    (is (= {{:?c 10} 1}
           (frequencies (query session cold-query))))

    ;; The derived fact should continue to exist after a retraction
    ;; since we used an unconditional insert.
    (is (= {{:?c 10} 1}
           (frequencies (query retracted-session cold-query))))))

(def-rules-test test-unconditional-insert-all

  {:rules [cold-lousy-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                            (insert-all-unconditional! [(->Cold ?t) (->LousyWeather)])]]

   :queries [cold-query [[] [[Cold (= ?c temperature)]]]
             lousy-query [[] [[?l <- LousyWeather]]]]

   :sessions [empty-session [cold-lousy-rule
                             cold-query
                             lousy-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    fire-rules)

        retracted-session (-> session
                              (retract (->Temperature 10 "MCI"))
                              fire-rules)]

    (is (= {{:?c 10} 1}
           (frequencies (query session cold-query))))

    (is (= {{:?l (->LousyWeather)} 1}
           (frequencies (query session lousy-query))))

    ;; The derived fact should continue to exist after a retraction
    ;; since we used an unconditional insert.
    (is (= {{:?c 10} 1}
           (frequencies (query retracted-session cold-query))))

    (is (= {{:?l (->LousyWeather)} 1}
           (frequencies (query retracted-session lousy-query))))))

(def-rules-test test-insert-retract-multi-input
  {:rules [cold-windy-rule [[[Temperature (< temperature 20) (= ?t temperature)]
                             [WindSpeed (> windspeed 30) (= ?w windspeed)]]
                            (insert! (->ColdAndWindy ?t ?w))]]

   :queries [cold-windy-query [[] [[ColdAndWindy (= ?ct temperature) (= ?cw windspeed)]]]]

   :sessions [empty-session [cold-windy-rule cold-windy-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (insert (->WindSpeed 40 "MCI"))
                    (fire-rules))

        retracted (-> session
                      (retract session (->Temperature 10 "MCI"))
                      fire-rules)]

    (is (= {{:?ct 10 :?cw 40} 1}
           (frequencies (query session cold-windy-query))))

    ;; Ensure retracting the temperature also removes the logically inserted fact.
    (is (empty?
         (query retracted cold-windy-query)))))

(def-rules-test test-retract-inserted-during-rule
  {:rules [history [[[?temps <- (acc/distinct) :from [Temperature]]]
                    (insert! (->TemperatureHistory ?temps))]

           history-low-salience [[[?temps <- (acc/distinct) :from [Temperature]]]
                                 (insert! (->TemperatureHistory ?temps))
                                 {:salience -10}]

           ;; Rule only for creating data when fired to expose this bug.
           ;; See https://github.com/cerner/clara-rules/issues/54
           create-data [[]
                        (insert! (->Temperature 20 "MCI")
                                 (->Temperature 25 "MCI")
                                 (->Temperature 30 "SFO"))]]

   :queries [temp-query [[] [[?t <- TemperatureHistory]]]]

   ;; The bug this is testing dependend on rule order, so we test
   ;; multiple orders.
   :sessions [session1 [create-data temp-query history] {}
              session2 [history create-data temp-query] {}
              session3 [history temp-query create-data] {}
              session4 [temp-query create-data history] {}

              session5 [create-data temp-query history-low-salience] {}
              session6 [history-low-salience create-data temp-query] {}
              session7 [history-low-salience temp-query create-data] {}
              session8 [temp-query create-data history-low-salience] {}]}

  ;; We should match an empty list to start.
  (is (= [{:?t (->TemperatureHistory #{(->Temperature 20 "MCI")
                                       (->Temperature 25 "MCI")
                                       (->Temperature 30 "SFO")})}]
         
         (-> session1 fire-rules (query temp-query))
         (-> session2 fire-rules (query temp-query))
         (-> session3 fire-rules (query temp-query))
         (-> session4 fire-rules (query temp-query))
         
         (-> session5 fire-rules (query temp-query))
         (-> session6 fire-rules (query temp-query))
         (-> session7 fire-rules (query temp-query))
         (-> session8 fire-rules (query temp-query)))))

(def-rules-test test-retracting-many-logical-insertions-for-same-rule

  {:rules [;; Do a lot of individual logical insertions for a single rule firing to
           ;; expose any StackOverflowError potential of stacking lazy evaluations in working memory.
           cold-temp  [[[Temperature (< temperature 30) (= ?t temperature)]]
                       ;; Many insertions based on the Temperature fact.
                       (dotimes [i 6000]
                         (insert! (->Cold (- ?t i))))]]

   :queries [;; Note: Adding a binding to this query, such as [Cold (= ?t temperature)]
             ;; will cause poor performance (really slow) for 6K retractions.
             ;; This is due to an issue with how retractions on elements is done at a per-grouped
             ;; on :bindings level.  If every Cold fact has a different temperature, none of them
             ;; share a :bindings when retractions happen.  This causes a lot of seperate, expensive
             ;; retractions in the network.
             ;; We need to find a way to do this in batch when/if possible.
             cold-query  [[] [[Cold]]]]

   :sessions [empty-session [cold-temp cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    fire-rules)]

    ;; Show the initial state for a sanity check.
    (is (= 6000
           (count (query session cold-query))))

    ;; Retract the Temperature fact that supports all of the
    ;; logical insertions.  This would trigger a StackOverflowError
    ;; if the insertions were stacked lazily "from the head".
    (is (= 0
           (count (query (-> session
                             (retract (->Temperature 10 "MCI"))
                             fire-rules)
                         cold-query))))))

(def-rules-test test-duplicate-insertions-with-only-one-removed

  {:rules [r [[[ColdAndWindy (= ?t temperature)]]
              (insert! (->Cold ?t))]]

   :queries [q [[]
                [[Cold (= ?t temperature)]]]]

   :sessions [empty-session [r q] {}]}

  (is (= (-> empty-session
             (insert (->ColdAndWindy 10 10))
             (insert (->ColdAndWindy 10 10))
             (fire-rules)
             (retract (->ColdAndWindy 10 10))
             (fire-rules)
             (query q))
         [{:?t 10}])
      "Removal of one duplicate fact that causes an immediately downstream rule to fire should not
         retract insertions that were due to other duplicate facts."))

(def-rules-test test-tiered-identical-insertions-with-retractions
  ;; The idea here is to test the behavior when the retraction
  ;; of a single token should cause multiple tokens to be retracted.

  {:rules [r1 [[[First]]
               (insert! (->Second) (->Second))]

           r2 [[[Second]]
               (insert! (->Third))]]

   :queries [third-query [[] [[Third]]]
             second-query [[] [[Second]]]]

   :sessions [empty-session [r1 r2 third-query second-query] {}]}

  (let [none-retracted-session (-> empty-session
                                   (insert (->First) (->First))
                                   (fire-rules))

        one-retracted-session (-> none-retracted-session
                                  (retract (->First))
                                  fire-rules)

        both-retracted-session (-> one-retracted-session
                                   (retract (->First))
                                   fire-rules)]

    (is (= (query none-retracted-session second-query)
           (query none-retracted-session third-query)
           [{} {} {} {}])
        "nothing retracted")
    (is (= (query one-retracted-session second-query)
           (query one-retracted-session third-query)
           [{} {}])
        "one First retracted")
    (is (= (query both-retracted-session second-query)
           (query both-retracted-session third-query))
        "Both First facts retracted")))

(def-rules-test duplicate-reasons-for-retraction-test

  {:rules [r1 [[[First]]
                           ;; As of writing the engine rearranges
                           ;; this so that the retraction comes last.
                           (do (retract! (->Cold 5))
                               (insert! (->Third)))]
           r2 [[[Second]
                            [:not [Third]]]
               (insert! (->Cold 5))]]

   :queries [q [[] [[Cold (= ?t temperature)]]]]

   :sessions [base-session [r1 r2 q] {}]}

  (is (= (-> base-session
               (insert (->Second))
               (fire-rules)
               (insert (->First))
               (fire-rules)
               (query q))
           [])
        "A retraction that becomes redundant after reordering of insertions
         and retractions due to batching should not cause failure."))

(def-rules-test test-remove-pending-activation-with-equal-previous-insertion
  ;; See issue 250 for details of the bug fix this tests.
  {:rules [lousy-weather-rule [[[?cw <- ColdAndWindy]]
                               (insert! (->LousyWeather))]]

   :queries [lousy-weather-query [[] [[LousyWeather]]]]

   :sessions [empty-session [lousy-weather-rule lousy-weather-query] {}]}

  (let [;; Test paths in remove-activations! that only match on facts that are equal by
        ;; value but not by reference.
        end-session-equal-facts (-> empty-session
                                    (insert (->ColdAndWindy 10 10))
                                    fire-rules
                                    (insert (->ColdAndWindy 10 10))
                                    (retract (->ColdAndWindy 10 10))
                                    fire-rules)

        ;; Test paths in remove-activations! in LocalMemory that only match on facts that are
        ;; equal by reference.
        end-session-identical-facts (let [fact (->ColdAndWindy 10 10)]
                                      (-> empty-session
                                          (insert fact)
                                          fire-rules
                                          (insert fact)
                                          (retract fact)
                                          fire-rules))]

    (is (= (query end-session-equal-facts lousy-weather-query)
           (query end-session-identical-facts lousy-weather-query)
           [{}]))))
