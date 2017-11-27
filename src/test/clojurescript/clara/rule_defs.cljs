(ns clara.rule-defs
  (:require-macros [cljs.test :refer (is deftest run-tests testing)]
                   [clara.test-rules-data])
  (:require [cljs.test :as t]
            [clara.rules.engine :as eng]
            [clara.rules.accumulators :as acc]
            [clara.rules :refer [insert retract fire-rules query insert!]
             :refer-macros [defrule defsession defquery]]
            [clara.rules.testfacts :as tf]))

(def simple-defrule-side-effect (atom nil))
(def other-defrule-side-effect (atom nil))

(defrule test-rule
         [tf/Temperature (< temperature 20)]
         =>
         (reset! other-defrule-side-effect ?__token__)
         (reset! simple-defrule-side-effect ?__token__))

(defquery cold-query
          []
          [tf/Temperature (< temperature 20) (== ?t temperature)])

;; Accumulator for getting the lowest temperature.
(def lowest-temp (acc/min :temperature))

(defquery coldest-query
          []
          [?t <- lowest-temp :from [tf/Temperature]])

(defrule is-cold-and-windy
         "Rule to determine whether it is indeed cold and windy."

         (tf/Temperature (< temperature 20) (== ?t temperature))
         (tf/WindSpeed (> windspeed 30) (== ?w windspeed))
         =>
         (insert! (tf/->ColdAndWindy ?t ?w)))

(defquery find-cold-and-windy
          []
          [?fact <- tf/ColdAndWindy])

