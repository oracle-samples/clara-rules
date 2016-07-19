(ns clara.tools.test-tracing
  (:require [clara.rules :refer :all]
            [clara.tools.tracing :as t]
            [clara.rules.engine :as eng]
            [clara.rules.dsl :as dsl]
            [clara.rules.accumulators :as acc]
            [clara.rules.testfacts :refer :all]
            [clojure.test :refer :all])

  (import [clara.rules.testfacts Temperature WindSpeed Cold Hot TemperatureHistory
           ColdAndWindy LousyWeather First Second Third Fourth]))

(deftest test-tracing-toggle
  (let [cold-rule (dsl/parse-rule [[Temperature (< temperature 20)]]
                                  (println "It's cold!"))

        session (mk-session [cold-rule])]

    (is (= false (t/is-tracing? session)))

    (is (= true (t/is-tracing? (-> session
                                   (t/with-tracing)))))

    (is (= false (t/is-tracing? (-> session
                                    (t/with-tracing)
                                    (t/without-tracing)))))))

(deftest test-simple-trace
  (let [rule-output (atom nil)
        cold-rule (dsl/parse-rule [[Temperature (< temperature 20)]]
                                  (reset! rule-output ?__token__))

        session (-> (mk-session [cold-rule])
                    (t/with-tracing)
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))]

    ;; Ensure expected events occur in order.
    (is (= [:add-facts :right-activate :left-activate :add-activations]
           (map :type (t/get-trace session))))))

(deftest test-rhs-retraction-trace
  (let [cold-rule (dsl/parse-rule [[Temperature (< temperature 20)]]
                                  (retract! (->Hot :too-hot)))
        hot-query (dsl/parse-query [] [[?hot <- Hot]])
        session (-> (mk-session [cold-rule hot-query])
                    (t/with-tracing)
                    (insert (->Hot :too-hot))
                    (fire-rules)
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))]
    (is (= (map :type (t/get-trace session))
           [:add-facts :right-activate :left-activate
            :add-facts :right-activate :left-activate
            :add-activations :retract-facts :right-retract :left-retract])
        "Validate that a retract! call in the RHS side of a rule appears in the trace
         before the :right-retract")))
        
(deftest test-accumulate-trace
  (let [lowest-temp (acc/min :temperature :returns-fact true)
        coldest-query (dsl/parse-query [] [[?t <- lowest-temp from [Temperature]]])

        session (-> (mk-session [coldest-query])
                    (t/with-tracing)
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    (is (= [:add-facts :accum-reduced :left-activate
            :add-facts :accum-reduced :left-retract
            :left-activate :add-facts :accum-reduced]

           (map :type (t/get-trace session)))))

  (testing "remove-accum-reduced"
    (let [all-temps (dsl/parse-query [] [[?t <- (acc/all) from [Temperature]]])
          
          session (-> (mk-session [all-temps])
                      (t/with-tracing)
                      fire-rules
                      (insert (->Temperature 15 "MCI"))
                      fire-rules)]

      (is (= [:add-facts :accum-reduced :left-retract :left-activate]

             (map :type (t/get-trace session)))))))

(deftest test-insert-trace
 (let [cold-rule (dsl/parse-rule [[Temperature (= ?temperature temperature) (< temperature 20)]]
                                  (insert! (->Cold ?temperature)))

        session (-> (mk-session [cold-rule])
                    (t/with-tracing)
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))]

    ;; Ensure expected events occur in order.
    (is (= [:add-facts :right-activate :left-activate :add-activations :add-facts-logical]
           (map :type (t/get-trace session))))))

(deftest test-insert-and-retract-trace
 (let [cold-rule (dsl/parse-rule [[Temperature (= ?temperature temperature) (< temperature 20)]]
                                  (insert! (->Cold ?temperature)))

        session (-> (mk-session [cold-rule] :cache false)
                    (t/with-tracing)
                    (insert (->Temperature 10 "MCI")
                            (->Temperature 20 "MCI"))
                    (fire-rules)
                    (retract (->Temperature 10 "MCI"))
                    (fire-rules))

       session-trace (t/get-trace session)]

    ;; Ensure expected events occur in order.
   (is (= [:add-facts :right-activate :left-activate :add-activations :add-facts-logical
           :retract-facts :right-retract :left-retract :remove-activations :retract-facts-logical]
          (map :type session-trace)))

   ;; Ensure only the expected fact was indicated as retracted.
   (let [retraction (first (filter #(= :retract-facts-logical (:type %)) session-trace))]
     (is (= [(->Cold 10)] (:facts retraction))))))
