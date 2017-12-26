(ns clara.rule-defs
  (:require [clara.rules.accumulators :as acc]
            [clara.rules.testfacts :as tf]
            [clara.tools.testing-utils :as tu]
    #?(:clj [clara.rules :refer [defrule defquery insert!]])
    #?(:cljs [clara.rules :refer-macros [defrule defquery] :refer [insert!]]))
  #?(:clj
     (:import [clara.rules.testfacts Temperature WindSpeed ColdAndWindy])))

;; Rule definitions used for tests in clara.test-rules-require.

(defrule test-rule
           [?t <- #?(:clj Temperature :cljs tf/Temperature) (< temperature 20)]
           =>
           (reset! tu/side-effect-holder ?t))

(defquery cold-query
          []
          [#?(:clj Temperature :cljs tf/Temperature) (< temperature 20) (== ?t temperature)])

;; Accumulator for getting the lowest temperature.
(def lowest-temp (acc/min :temperature))

(defquery coldest-query
          []
          [?t <- lowest-temp :from [#?(:clj Temperature :cljs tf/Temperature)]])

(defrule is-cold-and-windy
         "Rule to determine whether it is indeed cold and windy."

         (#?(:clj Temperature :cljs tf/Temperature) (< temperature 20) (== ?t temperature))
         (#?(:clj WindSpeed :cljs tf/WindSpeed) (> windspeed 30) (== ?w windspeed))
         =>
         (insert! (tf/->ColdAndWindy ?t ?w)))

(defquery find-cold-and-windy
          []
          [?fact <- #?(:clj ColdAndWindy :cljs tf/ColdAndWindy)])

