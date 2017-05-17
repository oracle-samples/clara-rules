(ns clara.test-complex-negation
  "Tests that validate that we wrap the fact-type-fn and ancestors-fn so that Clara's internal
   facts, for example NegationResult facts (added to fix issue 149) are not provided to user-provided
   custom fact-type-fn or ancestors-fn functions."
  (:require [clara.rules
             :refer-macros [defquery
                            defsession]
             :refer [query
                     insert
                     fire-rules]]
            [clara.rules.testfacts :refer [Temperature ->Temperature
                                           WindSpeed ->WindSpeed
                                           Cold ->Cold]]
            [cljs.test :as t]
            [cljs.test :refer-macros [run-tests
                                      deftest
                                      is]
             :include-macros true]))

(defquery negation-inside-negation-query
  []
  [:windspeed (= ?l (:location this))]
  [:not [:and
         [?t <- :temperature (= ?l (:location this))]
         [:not [:cold (= (:temperature this) (:temperature ?t))]]]])

;; Use ancestors of the fact types to ensure that the custom ancestors-fn
;; is used and that its arguments are the types from the custom fact-type-fn
(defquery negation-inside-negation-ancestors-query
  []
  [:windspeed-ancestor (= ?l (:location this))]
  [:not [:and
         [?t <- :temperature-ancestor (= ?l (:location this))]
         [:not [:cold-ancestor (= (:temperature this) (:temperature ?t))]]]])

(defn type->keyword
  [fact]
  (cond
    (instance? WindSpeed fact) :windspeed
    (instance? Temperature fact) :temperature
    (instance? Cold fact) :cold
    ;; If we reach the :else case then we are probably calling the user-provided :fact-type-fn
    ;; on an internal NegationResult fact which we should not do; see issue 241.
    :else (throw (ex-info "A fact that is not a WindSpeed, Temperature, or Cold was provided."
                          {:fact fact}))))

(defn keyword->ancestors
  [type-key]
  (condp = type-key
    :windspeed #{:windspeed-ancestor}
    :temperature #{:temperature-ancestor}
    :cold #{:cold-ancestor}

    (throw "A type that is not :windspeed, :temperature, or :cold was provided"
           {:type type})))
  
(defsession test-session 'clara.test-complex-negation
  :fact-type-fn type->keyword
  :ancestors-fn keyword->ancestors)

(deftest test-complex-negation
  (let [different-temps (-> test-session
                            (insert (->WindSpeed 10 "MCI")
                                    (->Temperature 10 "MCI")
                                    (->Cold 20))
                            (fire-rules))

        same-temps (-> test-session
                       (insert (->WindSpeed 10 "MCI")
                               (->Temperature 10 "MCI")
                               (->Cold 10))
                       (fire-rules))]
    (is (empty?
         (query different-temps negation-inside-negation-query)
         (query different-temps negation-inside-negation-ancestors-query)))

    (is (= [{:?l "MCI"}]
           (query same-temps negation-inside-negation-query)
           (query same-temps negation-inside-negation-ancestors-query)))))
