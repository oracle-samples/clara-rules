(ns clara.other-ruleset
  (:require-macros [clara.macros :refer [mk-rule defrule mk-query defquery mk-session]])
  (:require [clara.rules :refer [mk-rulebase insert retract fire-rules 
                                 query accumulate insert! retract! insert-unconditional!]]

            [clara.rules.testfacts :refer [->Temperature Temperature ->WindSpeed WindSpeed
                                           ->Cold Cold ->ColdAndWindy ColdAndWindy
                                           ->LousyWeather LousyWeather]]))

(defrule is-lousy
  (ColdAndWindy (= temperature 15))
  =>
  (insert! (->LousyWeather)))

;;; These rules are used for unit testing loading from a namespace.
(defquery subzero-locations
  "Query the subzero locations."
  []
  (Temperature (< temperature 0) (== ?loc location)))

(defquery temp-by-location
  "Query temperatures by location."
  [:?loc]
  (Temperature (== ?temp temperature) 
               (== ?loc location)))



