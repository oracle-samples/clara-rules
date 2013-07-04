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

(deftest test-simple-condition-binding
  (let [cold-query (new-query [(?t <-- Temperature (< temperature 20))])

        session (-> (rete-network) 
                    (add-query cold-query)
                    (new-session))]

    (insert session (->Temperature 15))
    (insert session (->Temperature 10))

    (is (= #{{:?t #clara.testfacts.Temperature{:temperature 15}} 
             {:?t #clara.testfacts.Temperature{:temperature 10}}}
           (into #{} (query session cold-query))))))

(deftest test-condition-and-value-binding
  (let [cold-query (new-query [(?t <-- Temperature (< temperature 20) (== ?v temperature))])

        session (-> (rete-network) 
                    (add-query cold-query)
                    (new-session))]

    (insert session (->Temperature 15))
    (insert session (->Temperature 10))

    ;; Ensure the condition's fact and values are all bound.
    (is (= #{{:?v 10, :?t #clara.testfacts.Temperature{:temperature 10}} 
             {:?v 15, :?t #clara.testfacts.Temperature{:temperature 15}}}
           (into #{} (query session cold-query))))))

(deftest test-simple-negation
  (let [not-cold-query (new-query [(not (Temperature (< temperature 20)))])

        session (-> (rete-network) 
                    (add-query not-cold-query)
                    (new-session))]

    ;; No facts for the above criteria exist, so we should see a positive result
    ;; with no bindings.
    (is (= #{{}}
           (into #{} (query session not-cold-query))))
    
    ;; Inserting an item into the sesion should invalidate the negation.
    (insert session (->Temperature 10))
    (is (empty? (query session not-cold-query)))

    ;; Retracting the inserted item should make the negation valid again.
    (retract session (->Temperature 10))
    (is (= #{{}}
           (into #{} (query session not-cold-query))))))


(deftest test-negation-with-other-conditions
  (let [windy-but-not-cold-query (new-query [(WindSpeed (> windspeed 30) (== ?w windspeed))
                                             (not (Temperature (< temperature 20)))])

        session (-> (rete-network) 
                    (add-query windy-but-not-cold-query)
                    (new-session))]

    ;; Make it windy, so our query should indicate that.
    (insert session (->WindSpeed 40))
    (is (= #{{:?w 40}}
           (into #{} (query session windy-but-not-cold-query))))

    ;; Make it hot and windy, so our query should still succeed.
    (insert session (->Temperature 80))
    (is (= #{{:?w 40}}
           (into #{} (query session windy-but-not-cold-query))))

    ;; Make it cold, so our query should return nothing.
    (insert session (->Temperature 10))
    (is (empty? (query session windy-but-not-cold-query)))))


(deftest test-negated-conjunction
  (let [not-cold-and-windy (new-query [(not (and (WindSpeed (> windspeed 30))
                                                 (Temperature (< temperature 20))))])

        session (-> (rete-network) 
                    (add-query not-cold-and-windy)
                    (new-session))]

    ;; It is not cold and windy, so we should have a match.
    (is (= #{{}}
           (into #{} (query session not-cold-and-windy))))

    ;; Make it cold and windy, so there should be no match.
    (insert session (->WindSpeed 40))
    (insert session (->Temperature 10))
    (is (empty? (query session not-cold-and-windy)))))

(deftest test-negated-disjunction
  (let [not-cold-or-windy (new-query [(not (or (WindSpeed (> windspeed 30))
                                               (Temperature (< temperature 20))))])

        session (-> (rete-network) 
                    (add-query not-cold-or-windy)
                    (new-session))]

    ;; It is not cold and windy, so we should have a match.
    (is (= #{{}}
           (into #{} (query session not-cold-or-windy))))

    ;; Make it cold and windy, so there should be no match.
    (insert session (->WindSpeed 40))
    (is (empty? (query session not-cold-or-windy)))

    ;; Retract the added fact and ensure we now match something.
    (retract session (->WindSpeed 40))
    (is (= #{{}}
           (into #{} (query session not-cold-or-windy))))
    ))


(deftest test-simple-retraction
  (let [cold-query (new-query [[Temperature (< temperature 20) (== ?t temperature)]])

        session (-> (rete-network) 
                    (add-query cold-query)
                    (new-session))
        temp (->Temperature 10)]

    ;; Ensure the item is there as expected.
    (insert session temp)
    (is (= #{{:?t 10}}
           (into #{} (query session cold-query))))

    ;; Ensure the item is retracted as expected.
    (retract session temp)    
    (is (= #{}
           (into #{} (query session cold-query))))))

(deftest test-noop-retraction
  (let [cold-query (new-query [[Temperature (< temperature 20) (== ?t temperature)]])

        session (-> (rete-network) 
                    (add-query cold-query)
                    (new-session))]

    ;; Ensure retracting a non-existant item has no ill effects.
    (insert session (->Temperature 10))
    (retract session (->Temperature 15))
    (is (= #{{:?t 10}}
           (into #{} (query session cold-query))))))

(deftest test-retraction-of-join
  (let [same-wind-and-temp (new-query [(Temperature (== ?t temperature))
                                      (WindSpeed (== ?t windspeed))])

        session (-> (rete-network) 
                    (add-query same-wind-and-temp)
                    (new-session))]

    (insert session (->Temperature 10))
    (insert session (->WindSpeed 10))

    ;; Ensure expected join occurred.
    (is (= #{{:?t 10}}
           (into #{} (query session same-wind-and-temp))))

    ;; Ensure item was removed as viewed by the query.
    (retract session (->Temperature 10))
    (is (= #{}
           (into #{} (query session same-wind-and-temp))))))


(deftest test-simple-disjunction
  (let [or-query (new-query [(or (Temperature (< temperature 20) (== ?t temperature))
                                 (WindSpeed (> windspeed 30) (== ?w windspeed)))])

        network (-> (rete-network) 
                    (add-query or-query))

        cold-session (new-session network)
        windy-session (new-session network)]

    (insert cold-session (->Temperature 15))
    (insert windy-session (->WindSpeed 50))

    (is (= #{{:?t 15}}
           (into #{} (query cold-session or-query))))

    (is (= #{{:?w 50}}
           (into #{} (query windy-session or-query))))))

(deftest test-disjunction-with-nested-and

  (let [really-cold-or-cold-and-windy 
        (new-query [(or (Temperature (< temperature 0) (== ?t temperature))
                        (and (Temperature (< temperature 20) (== ?t temperature))
                             (WindSpeed (> windspeed 30) (== ?w windspeed))))])

        network (-> (rete-network) 
                    (add-query really-cold-or-cold-and-windy))

        cold-session (new-session network)
        windy-session (new-session network)]

    (insert cold-session (->Temperature -10))
    (insert windy-session (->Temperature 15))
    (insert windy-session (->WindSpeed 50))

    (is (= #{{:?t -10}}
           (into #{} (query cold-session really-cold-or-cold-and-windy))))

    (is (= #{{:?w 50 :?t 15}}
           (into #{} (query windy-session really-cold-or-cold-and-windy))))))



(deftest test-ast-to-dnf 

  ;; Test simple condition.
  (is (= {:type :condition :content :placeholder} 
         (ast-to-dnf {:type :condition :content :placeholder} )))

  ;; Test single-item conjunection.
  (is (= {:type :and 
            :content [{:type :condition :content :placeholder}]} 
         (ast-to-dnf {:type :and 
                      :content [{:type :condition :content :placeholder}]})))

  ;; Test multi-item conjunction.
  (is (= {:type :and 
            :content [{:type :condition :content :placeholder1}
                      {:type :condition :content :placeholder2}
                      {:type :condition :content :placeholder3}]} 
         (ast-to-dnf {:type :and 
                      :content [{:type :condition :content :placeholder1}
                                {:type :condition :content :placeholder2}
                                {:type :condition :content :placeholder3}]})))
  
  ;; Test simple disjunction
  (is (= {:type :or
          :content [{:type :condition :content :placeholder1}
                    {:type :condition :content :placeholder2}
                    {:type :condition :content :placeholder3}]}
         (ast-to-dnf {:type :or
                      :content [{:type :condition :content :placeholder1}
                                {:type :condition :content :placeholder2}
                                {:type :condition :content :placeholder3}]})))


  ;; Test simple disjunction with nested conjunction.
  (is (= {:type :or
          :content [{:type :condition :content :placeholder1}
                    {:type :and
                     :content [{:type :condition :content :placeholder2}
                               {:type :condition :content :placeholder3}]}]}
         (ast-to-dnf {:type :or
                      :content [{:type :condition :content :placeholder1}
                                {:type :and
                                 :content [{:type :condition :content :placeholder2}
                                           {:type :condition :content :placeholder3}]}]}))) 

  ;; Test simple distribution of a nested or expression.
  (is (= {:type :or,
          :content
          [{:type :and,
            :content
            [{:content :placeholder1, :type :condition}
             {:content :placeholder3, :type :condition}]}
           {:type :and,
            :content
            [{:content :placeholder2, :type :condition}
             {:content :placeholder3, :type :condition}]}]}

         (ast-to-dnf {:type :and
                      :content 
                      [{:type :or 
                        :content 
                        [{:type :condition :content :placeholder1}
                         {:type :condition :content :placeholder2}]}                                 
                       {:type :condition :content :placeholder3}]})))

  ;; Test push negation to edges.
  (is (= {:type :and, 
           :content 
           [{:type :not, :content [{:content :placeholder1, :type :condition}]} 
            {:type :not, :content [{:content :placeholder2, :type :condition}]} 
            {:type :not, :content [{:content :placeholder3, :type :condition}]}]}
          (ast-to-dnf {:type :not 
                       :content
                       [{:type :or 
                         :content 
                         [{:type :condition :content :placeholder1}
                          {:type :condition :content :placeholder2}
                          {:type :condition :content :placeholder3}]}]})))

  (is (= {:type :and,
          :content [{:type :and,
                     :content
                     [{:type :not, :content [{:content :placeholder1, :type :condition}]}
                      {:type :not, :content [{:content :placeholder2, :type :condition}]}]}]}
         
       (ast-to-dnf {:type :and
                      :content
                      [{:type :not
                        :content
                        [{:type :or
                          :content
                          [{:type :condition :content :placeholder1}
                           {:type :condition :content :placeholder2}]}]}]})))
  
  ;; Test push negation to edges.
  (is (= {:type :or, 
          :content [{:type :not, :content [{:content :placeholder1, :type :condition}]} 
                    {:type :not, :content [{:content :placeholder2, :type :condition}]} 
                    {:type :not, :content [{:content :placeholder3, :type :condition}]}]}
         (ast-to-dnf {:type :not 
                      :content
                      [{:type :and
                        :content 
                        [{:type :condition :content :placeholder1}
                         {:type :condition :content :placeholder2}
                         {:type :condition :content :placeholder3}]}]})))

  ;; Test simple identity disjunction.
  (is (= {:type :or
          :content
          [{:type :not :content [{:type :condition :content :placeholder1}]}
           {:type :not :content [{:type :condition :content :placeholder2}]}]}
         (ast-to-dnf {:type :or
                      :content
                      [{:type :not :content [{:type :condition :content :placeholder1}]}
                       {:type :not :content [{:type :condition :content :placeholder2}]}]})))

  ;; Test distribution over multiple and expressions.
  (is (= {:type :or,
          :content
          [{:type :and,
            :content
            [{:content :placeholder1, :type :condition}
             {:content :placeholder4, :type :condition}
             {:content :placeholder5, :type :condition}]}
           {:type :and,
            :content
            [{:content :placeholder2, :type :condition}
             {:content :placeholder4, :type :condition}
             {:content :placeholder5, :type :condition}]}
           {:type :and,
            :content
            [{:content :placeholder3, :type :condition}
             {:content :placeholder4, :type :condition}
             {:content :placeholder5, :type :condition}]}]}
         (ast-to-dnf {:type :and
                      :content 
                      [{:type :or 
                        :content 
                        [{:type :condition :content :placeholder1}
                         {:type :condition :content :placeholder2}
                         {:type :condition :content :placeholder3}]}                                 
                       {:type :condition :content :placeholder4}
                       {:type :condition :content :placeholder5}]}))))
