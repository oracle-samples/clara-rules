(ns clara.test-salience
  (:require-macros [cljs.test :refer (is deftest run-tests testing)]
    [clara.rules.test-rules-data])
  (:require [cljs.test :as t]
    [clara.rules.engine :as eng]
    [clara.rules.accumulators :as acc]
    [clara.rules :refer [insert retract fire-rules query insert!]
     :refer-macros [defrule defsession defquery]]
    [clara.rules.testfacts :refer [->Temperature Temperature
                                   ->WindSpeed WindSpeed
                                   ->ColdAndWindy ColdAndWindy]]))

(def salience-rule-output (atom []))

(defrule salience-rule1
  {:salience 100}
  [Temperature]
  =>
  (swap! salience-rule-output conj 100))

(defrule salience-rule2
  {:salience 50}
  [Temperature]
  =>
  (swap! salience-rule-output conj 50))

(defrule salience-rule3
  {:salience 0}
  [Temperature]
  =>
  (swap! salience-rule-output conj 0))

(defrule salience-rule4
  {:salience -50}
  [Temperature]
  =>
  (swap! salience-rule-output conj -50))


(deftest test-salience
  (doseq [[sort-fn
           group-fn
           expected-order]

          [[:default-sort :default-group :forward-order]
           [:default-sort :salience-group :forward-order]
           [:default-sort :neg-salience-group :backward-order]

           [:numeric-greatest-sort :default-group :forward-order]
           [:numeric-greatest-sort :salience-group :forward-order]
           [:numeric-greatest-sort :neg-salience-group :backward-order]


           [:boolean-greatest-sort :default-group :forward-order]
           [:boolean-greatest-sort :salience-group :forward-order]
           [:boolean-greatest-sort :neg-salience-group :backward-order]


           [:numeric-least-sort :default-group :backward-order]
           [:numeric-least-sort :salience-group :backward-order]
           [:numeric-least-sort :neg-salience-group :forward-order]

           [:boolean-least-sort :default-group :backward-order]
           [:boolean-least-sort :salience-group :backward-order]
           [:boolean-least-sort :neg-salience-group :forward-order]]]

    (let [numeric-greatest-sort (fn [x y]
                                  (cond
                                    (= x y) 0
                                    (> x y) -1
                                    :else 1))

          numeric-least-sort (fn [x y]
                               (numeric-greatest-sort y x))

          salience-group-fn (fn [production]
                              (or (some-> production :props :salience)
                                0))

          neg-salience-group-fn (fn [p]
                                  (- (salience-group-fn p)))]

      ;; A CLJS macro that behaves like mk-session (creates a session but does not intern a Var)
      ;; has been proposed in #292. Internally, this would facilitate session generation for CLJS
      ;; tests such as this one, and may be useful if exposed publicly.

      (defsession test-salience-session 'clara.test-salience
                     :cache false
                     :activation-group-sort-fn (condp = sort-fn
                                                 :default-sort nil
                                                 :numeric-greatest-sort numeric-greatest-sort
                                                 :numeric-least-sort numeric-least-sort
                                                 :boolean-greatest-sort  >
                                                 :boolean-least-sort <)
                     :activation-group-fn (condp = group-fn
                                            :default-group nil
                                            :salience-group salience-group-fn
                                            :neg-salience-group neg-salience-group-fn))

      (reset! salience-rule-output [])

      (-> test-salience-session
        (insert (->Temperature 10 "MCI"))
        (fire-rules))

      (let [test-fail-str
            (str "Failure with sort-fn: " sort-fn ", group-fn: " group-fn ", and expected order: " expected-order)]
        (condp = expected-order
          :forward-order
          (is (= [100 50 0 -50] @salience-rule-output)
            test-fail-str)

          :backward-order
          (is (= [-50 0 50 100] @salience-rule-output)
            test-fail-str))))))
