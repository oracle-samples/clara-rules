;; These are tests that validate elementary function of the rules engine like
;; inserting and retracting facts.  They don't fit very well with specialized
;; test namespaces due to this simplicity.  This functionality is transitively
;; tested by numerous other tests, but there is some value in having direct tests
;; in case the complexity of those tests obscured a simpler issue.
#?(:clj
   (ns clara.test-simple-rules
     (:require [clara.tools.testing-utils :refer [def-rules-test
                                                  side-effect-holder] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert-all
                                    insert-all!
                                    insert!
                                    retract
                                    query]]

               [clara.rules.testfacts :refer [->Temperature ->Cold ->WindSpeed
                                              ->ColdAndWindy ->LousyWeather]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature
               Cold
               WindSpeed
               ColdAndWindy
               LousyWeather]))

   :cljs
   (ns clara.test-simple-rules
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    insert-all
                                    insert-all!
                                    retract
                                    query]]
               [clara.rules.testfacts :refer [->Temperature Temperature
                                              ->Cold Cold
                                              ->WindSpeed WindSpeed
                                              ->ColdAndWindy ColdAndWindy
                                              ->LousyWeather LousyWeather]]
               [clara.rules.accumulators]
               [cljs.test]
               [schema.test :as st]
               [clara.tools.testing-utils :refer [side-effect-holder] :as tu])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))
(use-fixtures :each tu/side-effect-holder-fixture)

(defn- has-fact? [token fact]
  (some #{fact} (map first (:matches token))))

(def-rules-test test-simple-rule

  {:rules [cold-rule [[[Temperature (< temperature 20)]]
                      (reset! side-effect-holder ?__token__)]]

   :sessions [empty-session [cold-rule] {}]}

  (-> empty-session
      (insert (->Temperature 10 "MCI"))
      (fire-rules))

  (is (has-fact? @side-effect-holder (->Temperature 10 "MCI"))))

(def-rules-test test-simple-insert

  {:rules [cold-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                      (insert! (->Cold ?t))]]

   :queries [cold-query [[] [[Cold (= ?c temperature)]]]]

   :sessions [empty-session [cold-rule cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))]

    (is (= #{{:?c 10}}
           (set (query session cold-query))))))

(def-rules-test test-simple-insert-all

  {:rules [cold-lousy-rule [[[Temperature (< temperature 20) (= ?t temperature)]]
                            (insert-all! [(->Cold ?t) (->LousyWeather)])]]

   :queries [cold-lousy-query [[] [[Cold (= ?c temperature)]
                                   [LousyWeather]]]]

   :sessions [empty-session [cold-lousy-rule cold-lousy-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    (fire-rules))]

    (is (= #{{:?c 10}}
           (set (query session cold-lousy-query))))))

(def-rules-test test-multiple-condition-rule

  {:rules [cold-windy-rule [[[Temperature (< temperature 20)]
                             [WindSpeed (> windspeed 25)]]
                            (reset! side-effect-holder ?__token__)]]

   :sessions [empty-session [cold-windy-rule] {}]}

  (let [session (-> empty-session
                    (insert (->WindSpeed 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    fire-rules)]

    (is (has-fact? @side-effect-holder (->WindSpeed 30 "MCI")))
    (is (has-fact? @side-effect-holder (->Temperature 10 "MCI")))))

(def-rules-test test-simple-retraction

  {:queries [cold-query [[] [[Temperature (< temperature 20) (= ?t temperature)]]]]

   :sessions [empty-session [cold-query] {}]}

  (let [temp (->Temperature 10 "MCI")

        session (-> empty-session
                    (insert temp)
                    fire-rules)

        retracted-session (-> session
                              (retract temp)
                              fire-rules)]

    ;; Ensure the item is there as expected.
    (is (= #{{:?t 10}}
           (set (query session cold-query))))

    ;; Ensure the item is retracted as expected.
    (is (= #{}
           (set (query retracted-session cold-query))))))

(def-rules-test test-noop-retraction

  {:queries [cold-query [[] [[Temperature (< temperature 20) (= ?t temperature)]]]]

   :sessions [empty-session [cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 10 "MCI"))
                    ;; Ensure retracting a nonexistent item has no ill effects.
                    (retract (->Temperature 15 "MCI"))
                    fire-rules)]

    (is (= #{{:?t 10}}
           (set (query session cold-query))))))

(def-rules-test test-multiple-simple-rules

  {:rules [cold-rule [[[Temperature (< temperature 20)]]
                      (swap! side-effect-holder assoc :cold ?__token__)]

           windy-rule [[[WindSpeed (> windspeed 25)]]
                       (swap! side-effect-holder assoc :windy ?__token__)]]

   :sessions [empty-session [cold-rule windy-rule] {}]}

  (reset! side-effect-holder {})

  (let [session (-> empty-session
                    (insert (->WindSpeed 30 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    fire-rules)]

    ;; Check rule side effects contin the expected token.
    (is (has-fact? (:cold @side-effect-holder) (->Temperature 10 "MCI")))

    (is (has-fact? (:windy @side-effect-holder) (->WindSpeed 30 "MCI")))))

(def-rules-test test-multiple-rules-same-fact

  {:rules [cold-rule [[[Temperature (< temperature 20)]]
                      (swap! side-effect-holder assoc :cold ?__token__)]

           subzero-rule [[[Temperature (< temperature 0)]]
                         (swap! side-effect-holder assoc :subzero ?__token__)]]

   :sessions [empty-session [cold-rule subzero-rule] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature -10 "MCI"))
                    fire-rules)]

    (is (has-fact? (:cold @side-effect-holder) (->Temperature -10 "MCI") ))

    (is (has-fact? (:subzero @side-effect-holder) (->Temperature -10 "MCI")))))


;; A test to validate that nodes of different parents will not be shared.
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
