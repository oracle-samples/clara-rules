(ns clara.sample-ruleset
  (:use clara.rules
        clara.rules.testfacts)
  (:refer-clojure :exclude [==])
  (import [clara.rules.testfacts Temperature WindSpeed Cold ColdAndWindy]))

;;; These rules are used for unit testing loading from a namespace.
(defquery freezing-locations
  []
  (Temperature (< temperature 32) (== ?loc location)))

(defrule is-cold-and-windy
  (Temperature (< temperature 20) (== ?t temperature))
  (WindSpeed (> windspeed 30) (== ?w windspeed))
  =>
  (insert! (->ColdAndWindy ?t ?w)))

(defquery find-cold-and-windy
  []
  (?fact <-- ColdAndWindy))



