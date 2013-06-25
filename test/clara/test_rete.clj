(ns clara.test-rete
  (use clojure.test
       clara.rete
       clojure.pprint
       clara.testfacts)
  (:refer-clojure :exclude [==])
  (import [clara.testfacts Temperature WindSpeed]))

(deftest test-simple-rule
  (let [rule-output (atom nil)
        cold-rule (new-rule [[Temperature (< temperature 20)]] 
                            (reset! rule-output ?__token__) )

        session (-> (rete-network) 
                    (add-rule cold-rule)
                    (new-session))]

    (insert session (->Temperature 10))
    (fire-rules session)
    (is (= 
         (->Token [(->Temperature 10)] {})
         @rule-output))))


(deftest test-multiple-condition-rule
  (let [rule-output (atom nil)
        cold-windy-rule (new-rule [(Temperature (< temperature 20))
                                   (WindSpeed (> windspeed 25))] 
                                  (reset! rule-output ?__token__))
        session (-> (rete-network) 
                    (add-rule cold-windy-rule)
                    (new-session))]

    (insert session (->WindSpeed 30))
    (insert session (->Temperature 10))
    (fire-rules session)

    (is (= 
         (->Token [(->Temperature 10) (->WindSpeed 30)] {})
         @rule-output))))

(deftest test-multiple-simple-rules
  (let [cold-rule-output (atom nil)
        windy-rule-output (atom nil)
        cold-rule (new-rule [(Temperature (< temperature 20))] 
                            (reset! cold-rule-output ?__token__))
        windy-rule (new-rule [(WindSpeed (> windspeed 25))] 
                             (reset! windy-rule-output ?__token__))
        session (-> (rete-network) 
                    (add-rule cold-rule)
                    (add-rule windy-rule)
                    (new-session))]

    (insert session (->WindSpeed 30))
    (insert session (->Temperature 10))
    (fire-rules session)
    ;; Check rule side effects contin the expected token.
    (is (= 
         (->Token [(->Temperature 10)] {})
         @cold-rule-output))
    (is (= 
         (->Token [(->WindSpeed 30)] {})
         @windy-rule-output))))

(deftest test-simple-binding
  (let [rule-output (atom nil)
        cold-rule (new-rule [(Temperature (< temperature 20) (== ?t temperature))] 
                            (reset! rule-output ?t) )

        session (-> (rete-network) 
                    (add-rule cold-rule)
                    (new-session))]

    (insert session (->Temperature 10))
    (fire-rules session)
    (is (= 10 @rule-output))))

(deftest test-simple-join-binding 
  (let [rule-output (atom nil)
        same-wind-and-temp (new-rule [(Temperature (== ?t temperature))
                                      (WindSpeed (== ?t windspeed))] 
                                     (reset! rule-output ?t) )

        session (-> (rete-network) 
                    (add-rule same-wind-and-temp)
                    (new-session))]

    (insert session (->Temperature 10))
    (insert session (->WindSpeed 10))
    (fire-rules session)
    (is (= 10 @rule-output))))

(deftest test-simple-join-binding-nomatch
  (let [rule-output (atom nil)
        same-wind-and-temp (new-rule [(Temperature (== ?t temperature))
                                      (WindSpeed (== ?t windspeed))] 
                                     (reset! rule-output ?t) )

        session (-> (rete-network) 
                    (add-rule same-wind-and-temp)
                    (new-session))]

    (insert session (->Temperature 10))
    (insert session (->WindSpeed 20))
    (fire-rules session)
    (is (= nil @rule-output))))

(deftest test-simple-query
  (let [cold-query (new-query [(Temperature (< temperature 20) (== ?t temperature))])

        session (-> (rete-network) 
                    (add-query cold-query)
                    (new-session))]

    (insert session (->Temperature 15))
    (insert session (->Temperature 10))
    (insert session (->Temperature 80))

    ;; The query should identify all items that wer einserted and matchd the
    ;; expected criteria.
    (is (= #{{:?t 15} {:?t 10}}
           (into #{} (query session cold-query))))))

(comment
  (deftest parse-expression
    (pprint (parse-lhs '[(Temperature (< temperature 20) (== ?t temperature))
                         (Temperature (< temperature 50) (== ?t temperature))
                         (or (WindSpeed (> windspeed 25))
                             (WindSpeed (> windspeed 505)))]))))