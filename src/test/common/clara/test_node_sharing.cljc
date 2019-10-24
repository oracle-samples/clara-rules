#?(:clj
   (ns clara.test-node-sharing
     (:require [clara.tools.testing-utils :refer [def-rules-test] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert-all
                                    insert-unconditional!
                                    insert!
                                    retract
                                    query]]

               [clara.rules.testfacts :refer [->Temperature ->Cold ->WindSpeed
                                              ->ColdAndWindy]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature
               Cold
               WindSpeed
               ColdAndWindy
               WindSpeed]))

   :cljs
   (ns clara.test-node-sharing
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    insert-all
                                    insert-unconditional!
                                    retract
                                    query]]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->Cold Cold
                                              ->WindSpeed WindSpeed
                                              ->ColdAndWindy ColdAndWindy]]
               [clara.rules.accumulators]
               [cljs.test]
               [schema.test :as st]
               [clara.tools.testing-utils :as tu])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture) tu/side-effect-holder-fixture)

;; See issue 433 for more information
(def-rules-test test-or-sharing-same-condition
  {:rules [or-rule [[[:or
                      [::a]
                      [::b]]
                     [::d]]
                    (insert! {:fact-type ::c})]

           other-rule [[[::a]
                        [::d]]
                       (insert! {:fact-type ::e})]]
   :queries [c-query [[] [[?c <- ::c]]]
             e-query [[] [[?e <- ::e]]]]

   :sessions [empty-session [or-rule other-rule c-query e-query] {:fact-type-fn :fact-type}]}
  (let [session (-> empty-session
                    (insert {:fact-type ::b})
                    (insert {:fact-type ::d})
                    fire-rules)]

    (is (= (set (query session c-query))
           #{{:?c {:fact-type ::c}}}))

    (is (= (set (query session e-query))
           #{}))))

;; Replicate the same logical scenario as above in test-or-sharing-same-condition
;; in a single rule with a boolean :or condition. 
(def-rules-test test-or-sharing-same-condition-in-same-production
  {:rules [single-rule [[[:or
                          [:and [:or
                                 [::a]
                                 [::b]]
                           [::d]]
                          [:and
                           [::a]
                           [::d]]]]

                        (insert! {:fact-type ::c})]]

   :queries [c-query [[] [[?c <- ::c]]]]

   :sessions [empty-session [single-rule c-query] {:fact-type-fn :fact-type}]}

  (let [session (-> empty-session
                    (insert {:fact-type ::b})
                    (insert {:fact-type ::d})
                    fire-rules)]

    (is (= (query session c-query)
           [{:?c {:fact-type ::c}}]))))


;; FIXME: Log an issue for this bug and uncomment when it is resolved.  Since an :or
;; condition is essentially creating two rules I'd expect this to insert twice.
(comment
  (def-rules-test test-or-sharing-same-condition-in-same-production-duplicate-path
    {:rules [single-rule [[[:or
                            [:and [:or
                                   [::a]
                                   [::b]]
                             [::d]
                             ]
                            [:and
                             [::a]
                             [::d]]]]

                          (insert! {:fact-type ::c})]]

     :queries [c-query [[] [[?c <- ::c]]]]

     :sessions [empty-session [single-rule c-query] {:fact-type-fn :fact-type}]}

    (let [session (-> empty-session
                      (insert {:fact-type ::a})
                      (insert {:fact-type ::d})
                      fire-rules)]

      (is (= (query session c-query)
             [{:?c {:fact-type ::c}}
              {:?c {:fact-type ::c}}])))))

(def-rules-test test-or-sharing-same-condition-in-same-production-beginning-duplicate-path
  {:rules [single-rule [[[:or
                          [:and
                           [:or
                            [::a]
                            [::b]]
                           [::d]
                           [::e]]
                          [:and
                           [::a]
                           [::d]
                           [::f]]]]

                        (insert! {:fact-type ::c})]]

   :queries [c-query [[] [[?c <- ::c]]]]

   :sessions [empty-session [single-rule c-query] {:fact-type-fn :fact-type}]}

  (let [session (-> empty-session
                    (insert {:fact-type ::a})
                    (insert {:fact-type ::d})
                    (insert {:fact-type ::e})
                    (insert {:fact-type ::f})
                    fire-rules)]

    (is (= (query session c-query)
           [{:?c {:fact-type ::c}}
            {:?c {:fact-type ::c}}]))))

(def-rules-test test-or-sharing-same-condition-in-same-production-ending-duplicate-path
  {:rules [single-rule [[[:or
                          [:and
                           [::e]
                           [:or
                            [::a]
                            [::b]]
                           [::d]]
                          [:and
                           [::f]
                           [::a]
                           [::d]]]]

                        (insert! {:fact-type ::c})]]

   :queries [c-query [[] [[?c <- ::c]]]]

   :sessions [empty-session [single-rule c-query] {:fact-type-fn :fact-type}]}

  (let [session (-> empty-session
                    (insert {:fact-type ::a})
                    (insert {:fact-type ::d})
                    (insert {:fact-type ::e})
                    (insert {:fact-type ::f})
                    fire-rules)]

    (is (= (query session c-query)
           [{:?c {:fact-type ::c}}
            {:?c {:fact-type ::c}}]))))

(def-rules-test test-or-sharing-same-condition-with-different-intervening-conditions

  {:rules [or-rule [[[:or
                      [::a]
                      [::b]]
                     [::f]
                     [::d]]
                    (insert! {:fact-type ::c})]

           other-rule [[[::a]
                        [::g]
                        [::d]]
                       (insert! {:fact-type ::e})]]
   :queries [c-query [[] [[?c <- ::c]]]
             e-query [[] [[?e <- ::e]]]]

   :sessions [empty-session [or-rule other-rule c-query e-query] {:fact-type-fn :fact-type}]}

  (let [session (-> empty-session
                    (insert {:fact-type ::b})
                    (insert {:fact-type ::d})
                    (insert {:fact-type ::f})
                    fire-rules)]

    (is (= (set (query session c-query))
           #{{:?c {:fact-type ::c}}}))

    (is (= (set (query session e-query))
           #{}))))


(def-rules-test test-sharing-simple-condition-followed-by-or

  {:rules [cold-and-windy-rule [[[WindSpeed (> windspeed 20) (do (swap! tu/side-effect-holder inc)
                                                                 true)]
                                 [:or
                                  [Cold]
                                  [Temperature (< temperature 0)]]]
                                (insert! (->ColdAndWindy nil nil))]]

   :queries [cold-windy-query [[] [[?cw <- ColdAndWindy]]]]

   :sessions [empty-session [cold-and-windy-rule cold-windy-query] {}]}

  (reset! tu/side-effect-holder 0)
  (is (= (-> empty-session
             (insert (->WindSpeed 30 "LCY")
                     (->Cold -20))
             fire-rules
             (query cold-windy-query))
         [{:?cw (->ColdAndWindy nil nil)}]))
  (is (= @tu/side-effect-holder 1) "The constraints on WindSpeed should only be evaluated once"))

(def-rules-test test-shared-first-condition-multiple-rules-met

  {:rules [cold-fact-rule [[[WindSpeed (> windspeed 20) (do (swap! tu/side-effect-holder inc)
                                                            true)]
                            [Cold]]
                           (insert! (->ColdAndWindy nil nil))]

           temp-fact-rule [[[WindSpeed (> windspeed 20) (do (swap! tu/side-effect-holder inc)
                                                            true)]
                            [Temperature (< temperature 0)]]
                           (insert! (->ColdAndWindy nil nil))]]

   :queries [cold-windy-query [[] [[?cw <- ColdAndWindy]]]]

   :sessions [empty-session [cold-fact-rule temp-fact-rule cold-windy-query] {}]}

  (reset! tu/side-effect-holder 0)
  (is (= (-> empty-session
             (insert (->WindSpeed 30 "LCY")
                     (->Cold -20))
             fire-rules
             (query cold-windy-query))
         [{:?cw (->ColdAndWindy nil nil)}]))
  (is (= @tu/side-effect-holder 1) "The constraints on WindSpeed should only be evaluated once"))

(def-rules-test test-shared-second-condition-multiple-rules-unmet

  {:rules [cold-fact-rule [[[::absent-type-1]
                            [WindSpeed (> windspeed 20) (do (swap! tu/side-effect-holder inc)
                                                            true)]
                            [Cold]]
                           (insert! (->ColdAndWindy nil nil))]

           temp-fact-rule [[[::absent-type-2]
                            [WindSpeed (> windspeed 20) (do (swap! tu/side-effect-holder inc)
                                                            true)]
                            [Temperature (< temperature 0)]]
                           (insert! (->ColdAndWindy nil nil))]]

   :queries [cold-windy-query [[] [[?cw <- ColdAndWindy]]]]

   :sessions [empty-session [cold-fact-rule temp-fact-rule cold-windy-query] {}]}

  (reset! tu/side-effect-holder 0)
  (is (empty? (-> empty-session
                  (insert (->WindSpeed 30 "LCY")
                          (->Cold -20))
                  fire-rules
                  (query cold-windy-query))))
  ;; Note that even though the parent condition isn't met, the WindSpeed will still be evaluated
  ;; to determine if it meets the constraints since it isn't dependent on the parent condition.
  ;; It is possible that this would be change in the future if we decided to use lazy evaluation,
  ;; but in any case it should not be more than 1.
  (is (= @tu/side-effect-holder 1) "The constraints on WindSpeed should be evaluated exactly once"))
