(ns clara.test-durability
  (:require [clara.rules :refer :all]
            [clara.rules.dsl :as dsl]
            [clara.rules.durability :as d]
            [clara.rules.accumulators :as acc]
            [clara.rules.testfacts :refer :all]
            [clojure.test :refer :all]
            schema.test)
  (:import [clara.rules.testfacts Temperature Cold]))

(use-fixtures :once schema.test/validate-schemas)

(defn- restore-session
  "Wrapper for d/restore-session-state that validates the session can be serialized."
  [session session-state]
  (d/restore-session-state session (-> (pr-str session-state)
                                       (read-string))))

(defn- has-fact? [token fact]
  (some #{fact} (map first (:matches token))))

(deftest test-restore-simple-query

  (let [cold-query (dsl/parse-query [] [[?t <- Temperature (< temperature 20)]])

        session (-> (mk-session [cold-query])

                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))

        session-state (d/session-state session)

        restored-session (-> (mk-session [cold-query])
                             (restore-session session-state))]

    (is (= [{:?t (->Temperature 10 "MCI")}]
           (query session cold-query)))

    (is (= [{:?t (->Temperature 10 "MCI")}]
           (query restored-session cold-query)))))


(deftest test-restore-unfired-activation
  (let [rule-output (atom nil)
        cold-rule (dsl/parse-rule [[Temperature (< temperature 20)]]
                                  (reset! rule-output ?__token__))

        session-state (-> (mk-session [cold-rule])
                          (insert (->Temperature 10 "MCI"))
                          (d/session-state))]

    ;; Rule not yet fired, so the output should be nil.
    (is (nil? @rule-output))

    ;; Restore the session and run the rule.
    (-> (mk-session [cold-rule])
        (restore-session session-state)
        (fire-rules))

    (is (has-fact? @rule-output (->Temperature 10 "MCI")))))


(deftest test-restore-fired-activation
  (let [rule-output (atom nil)
        cold-rule (dsl/parse-rule [[Temperature (< temperature 20)]]
                                  (reset! rule-output ?__token__))

        session-state (-> (mk-session [cold-rule])
                          (insert (->Temperature 10 "MCI"))
                          (fire-rules)
                          (d/session-state))]

    ;; Rule fired previously, so the output should be in place.
    (is (has-fact? @rule-output (->Temperature 10 "MCI")))

    ;; Clear the output and restore the session. It should already be activated,
    ;; so the output is not updated again.
    (reset! rule-output nil)

    (-> (mk-session [cold-rule])
        (restore-session session-state)
        (fire-rules))

    (is (nil? @rule-output))))

(deftest test-restore-accum-result
  (let [lowest-temp (acc/min :temperature :returns-fact true)
        coldest-query (dsl/parse-query [] [[?t <- lowest-temp from [Temperature]]])

        session (-> (mk-session [coldest-query])
                    (insert (->Temperature 15 "MCI")
                            (->Temperature 10 "MCI")
                            (->Temperature 80 "MCI"))
                    (fire-rules))

        session-state (d/session-state session)

        restored-session (-> (mk-session [coldest-query])
                             (restore-session session-state))

        ]

    ;; Accumulator returns the lowest value.
    (is (= #{{:?t (->Temperature 10 "MCI")}}
           (set (query restored-session coldest-query))))))

(deftest test-restore-truth-maintenance
  (let [cold-rule (dsl/parse-rule [[Temperature (< temperature 20) (= ?temp temperature)]]
                                  (insert! (->Cold ?temp)))

        cold-query (dsl/parse-query [] [[?cold <- Cold]])

        empty-session (mk-session [cold-rule cold-query])

        session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))

        session-state (d/session-state session)

        restored-session (-> empty-session
                             (restore-session session-state))]

    (is (= [{:?cold (->Cold 10)}]
           (query restored-session cold-query)))

    ;; Ensure the retraction propagates and removes our inserted item.
    (is (= []
           (query (retract restored-session (->Temperature 10 "MCI")) cold-query)))))
