(ns clara.tools.test-tracing
  (:require [clara.rules :refer :all]
            [clara.tools.tracing :as t]
            [clara.rules.engine :as eng]
            [clara.rules.dsl :as dsl]
            [clara.rules.testfacts :refer :all]
            [clojure.test :refer :all])

  (import [clara.rules.testfacts Temperature WindSpeed Cold TemperatureHistory
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

(deftest test-accumulate-trace
  (let [lowest-temp (accumulate
                     :reduce-fn (fn [value item]
                                  (if (or (= value nil)
                                          (< (:temperature item) (:temperature value) ))
                                    item
                                    value)))
        coldest-query (dsl/parse-query [] [[?t <- lowest-temp from [Temperature]]])

        session (-> (mk-session [coldest-query])
                    (t/with-tracing)
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    (is (= [:add-facts :accum-reduced :left-activate :add-facts
            :left-retract :accum-reduced :left-activate :add-facts
            :left-retract :accum-reduced :left-activate]

           (map :type (t/get-trace session))))))

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
