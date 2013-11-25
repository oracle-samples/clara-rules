(ns clara.sample-ruleset
  (:require-macros [clara.macros :refer [mk-rule defrule mk-query defquery mk-session]])
  (:require [clara.rules :refer [mk-rulebase insert retract fire-rules 
                                 query accumulate insert! retract! insert-unconditional!]]

            [clara.rules.testfacts :refer [->Temperature Temperature ->WindSpeed WindSpeed
                                           ->Cold Cold ->ColdAndWindy ColdAndWindy
                                           ->LousyWeather LousyWeather]]))

;;; These rules are used for unit testing loading from a namespace.
(defquery freezing-locations
  "Query the freezing locations."
  []
  (Temperature (< temperature 32) (== ?loc location)))

(defrule is-cold-and-windy
  "Rule to determine whether it is indeed cold and windy."

  (Temperature (< temperature 20) (== ?t temperature))
  (WindSpeed (> windspeed 30) (== ?w windspeed))
  =>
  (insert! (->ColdAndWindy ?t ?w)))

(defquery find-cold-and-windy
  []
  (?fact <- ColdAndWindy))

(defquery find-lousy-weather
  []
  (?fact <- LousyWeather))



