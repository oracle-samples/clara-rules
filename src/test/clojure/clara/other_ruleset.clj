(ns clara.other-ruleset
  (:use clara.rules
        clara.rules.testfacts)
  (:refer-clojure :exclude [==])
  (:import [clara.rules.testfacts
            Temperature
            WindSpeed
            Cold
            ColdAndWindy
            LousyWeather]))

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



