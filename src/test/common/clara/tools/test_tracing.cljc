#?(:clj
   (ns clara.tools.test-tracing
       (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
                 [clara.rules :refer [fire-rules
                                      insert
                                      insert-all
                                      insert!
                                      retract
                                      retract!
                                      query]]
                 [clara.tools.tracing :as t]
                 [clara.rules.accumulators :as acc]
                 [clara.rules.testfacts :refer :all]
                 [clojure.test :refer :all])
     (:import [clara.rules.testfacts Temperature WindSpeed Cold Hot TemperatureHistory
               ColdAndWindy LousyWeather First Second Third Fourth]))

   :cljs
   (ns clara.tools.test-tracing
      (:require [clara.rules :refer [fire-rules
                                     insert
                                     insert!
                                     retract!
                                     insert-all
                                     retract
                                     query]]
                [clara.tools.tracing :as t]
                [clara.rules.accumulators :as acc]
                [clara.rules.testfacts :refer [->Temperature Temperature
                                               ->TemperatureHistory TemperatureHistory
                                               ->Hot Hot
                                               ->Cold Cold
                                               ->ColdAndWindy ColdAndWindy
                                               ->WindSpeed WindSpeed]])
      (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                       [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(def-rules-test test-tracing-toggle
  {:rules [cold-rule [[[Temperature (< temperature 20)]]
                      (println "It's cold!")]]

   :sessions [session [cold-rule] {}]}

  (is (= false (t/is-tracing? session)))

  (is (= true (t/is-tracing? (-> session
                                 (t/with-tracing)))))

  (is (= false (t/is-tracing? (-> session
                                  (t/with-tracing)
                                  (t/without-tracing))))))

(def rule-output (atom nil))
(def-rules-test test-simple-trace
  {:rules [cold-rule [[[Temperature (< temperature 20)]]
                      (reset! rule-output ?__token__)]]
   :sessions [empty-session [cold-rule] {}]}

  (let [session (-> empty-session
                    (t/with-tracing)
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))]

    ;; Ensure expected events occur in order.
    (is (= [:add-facts :alpha-activate :right-activate :left-activate :add-activations :fire-activation]
           (map :type (t/get-trace session))))))

(def-rules-test test-rhs-retraction-trace
  {:rules [cold-rule [[[Temperature (< temperature 20)]]
                      (retract! (->Hot :too-hot))]]
   :queries [hot-query [[]
                        [[?hot <- Hot]]]]
   :sessions [empty-session [cold-rule hot-query] {}]}

  (let [session (-> empty-session
                    (t/with-tracing)
                    (insert (->Hot :too-hot))
                    (fire-rules)
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))]
    (is (= (map :type (t/get-trace session))
           [:add-facts :alpha-activate :right-activate :left-activate
            :add-facts :alpha-activate :right-activate :left-activate
            :add-activations :fire-activation :retract-facts
            :alpha-retract :right-retract :left-retract])
        "Validate that a retract! call in the RHS side of a rule appears in the trace
         before the :right-retract")))
        
(def-rules-test test-accumulate-trace
  {:queries [coldest-query [[]
                            [[?t <- (acc/min :temperature :returns-fact true) from [Temperature]]]]]
   :sessions [empty-session [coldest-query] {}]}

  (let [session (-> empty-session
                    (t/with-tracing)
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)]

    (is (= [:add-facts :alpha-activate :right-activate :accum-reduced
            :left-activate :add-facts :alpha-activate :right-activate
            :accum-reduced :left-retract :left-activate :add-facts
            :alpha-activate :right-activate :accum-reduced]

           (map :type (t/get-trace session))))))

(def-rules-test remove-accum-reduced
  {:queries [all-temps [[]
                        [[?t <- (acc/all) from [Temperature]]]]]
   :sessions [empty-session [all-temps] {}]}

  (let [session (-> empty-session
                    (t/with-tracing)
                    fire-rules
                    (insert (->Temperature 15 "MCI"))
                    fire-rules)]

    (is (= [:add-facts :alpha-activate :right-activate :accum-reduced :left-retract :left-activate]

           (map :type (t/get-trace session))))))

(def-rules-test test-insert-trace
  {:rules [cold-rule [[[Temperature (= ?temperature temperature) (< temperature 20)]]
                      (insert! (->Cold ?temperature))]]
   :sessions [empty-session [cold-rule] {}]}

 (let [session (-> empty-session
                  (t/with-tracing)
                  (insert (->Temperature 10 "MCI"))
                  (fire-rules))]

    ;; Ensure expected events occur in order.
   (is (= [:add-facts :alpha-activate :right-activate :left-activate
           :add-activations :fire-activation :add-facts-logical :activation-group-transition]
           (map :type (t/get-trace session))))))

(def-rules-test test-insert-and-retract-trace
  {:rules [cold-rule [[[Temperature (= ?temperature temperature) (< temperature 20)]]
                      (insert! (->Cold ?temperature))]]
   :sessions [empty-session [cold-rule] {:cache false}]}

 (let [session (-> empty-session
                    (t/with-tracing)
                    (insert (->Temperature 10 "MCI")
                            (->Temperature 20 "MCI"))
                    (fire-rules)
                    (retract (->Temperature 10 "MCI"))
                    (fire-rules))

       session-trace (t/get-trace session)]

    ;; Ensure expected events occur in order.
   (is (= [:add-facts :alpha-activate :right-activate :left-activate :add-activations :fire-activation
           :add-facts-logical :activation-group-transition :retract-facts :alpha-retract :right-retract
           :left-retract :remove-activations :retract-facts-logical]
          (map :type session-trace)))

   ;; Ensure only the expected fact was indicated as retracted.
   (let [retraction (first (filter #(= :retract-facts-logical (:type %)) session-trace))]
     (is (= [(->Cold 10)] (:facts retraction))))))

(def-rules-test test-ranked-productions
  {:rules [temperature-rule [[[Temperature (= ?temperature temperature) (< temperature 20)]]
                             (insert! (->Cold ?temperature))]

           cold-and-windy-rule [[[ColdAndWindy (= ?temperature temperature) (< temperature 20)]]
                                (insert! (->Cold ?temperature))]]

   :sessions [empty-session [temperature-rule cold-and-windy-rule] {}]}

  (let [mostly-temp (-> empty-session
                        t/with-tracing
                        (insert (->Temperature 10 "MCI"))
                        fire-rules
                        (insert (->Temperature 10 "MCI"))
                        fire-rules
                        (insert (->ColdAndWindy 10 10))
                        fire-rules)

        mostly-temp-counts (t/ranked-productions mostly-temp)

        mostly-cold-and-windy (-> empty-session
                                  t/with-tracing
                                  (insert (->Temperature 10 "MCI"))
                                  fire-rules
                                  (insert (->ColdAndWindy 10 10))
                                  fire-rules
                                  (insert (->ColdAndWindy 10 10))
                                  fire-rules)

        mostly-cold-and-windy-counts (t/ranked-productions mostly-cold-and-windy)]

    (is (= (keys mostly-temp-counts)
           ["temperature-rule" "cold-and-windy-rule"]))

    (is (= (keys mostly-cold-and-windy-counts)
           ["cold-and-windy-rule" "temperature-rule"]))

    ;; Since the exact number of interactions is subject to change based on Clara's internal implementation details
    ;; we instead test the ratio of interaction counts instead, which should be more stable.  The pattern above
    ;; of inserting and firing a single fact at a time prevents fact batching from impacting these ratios.
    (is (= (mostly-temp-counts "temperature-rule")
           (* 2 (mostly-temp-counts "cold-and-windy-rule"))))

    (is  (= (mostly-cold-and-windy-counts "cold-and-windy-rule")
            (* 2 (mostly-cold-and-windy-counts "temperature-rule"))))))
