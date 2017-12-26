(ns clara.durability-rules
  (:require [clara.rules :refer :all]
            [clara.rules.accumulators :as acc]
            [clara.rules.testfacts :refer :all])
  (:import [clara.rules.testfacts
            Temperature
            WindSpeed
            Cold
            Hot
            TemperatureHistory]))

(defrecord Threshold [v])
(defrecord TempsUnderThreshold [ts])

(defrule find-cold
  "Rule with no joins"
  [Temperature (= ?t temperature) (< temperature 30)]
  =>
  (insert! (->Cold ?t)))

(defrule find-hot
  "Rule using NegationWithJoinFilterNode"
  [Temperature (= ?t temperature)]
  [:not [Cold (>= temperature ?t)]]
  =>
  (insert! (->Hot ?t)))

(defrecord UnpairedWindSpeed [ws])

(defrule find-wind-speeds-without-temp
  "Rule using NegationNode"
  [?w <- WindSpeed
   ;; Empty constraint and a constraint containing an empty list to test serializing an EmptyList,
   ;; see Issue 352 for more information
   ()
   (not= this ())
   (= ?loc location)]
  [:not [Temperature (= ?loc location)]]
  =>
  (insert! (->UnpairedWindSpeed ?w)))

(defrule find-temps
  "Rule using AccumulateNode and TestNode"
  [?ts <- (acc/all) :from [Temperature]]
  [:test (not-empty ?ts)]
  =>
  (insert! (->TemperatureHistory (mapv :temperature ?ts))))

(defrule find-temps-under-threshold
  "Rule using AccumulateWithJoinFilterNode"
  [Threshold (= ?v v)]
  [?ts <- (acc/all) :from [Temperature (< temperature ?v)]]
  =>
  (insert! (->TempsUnderThreshold ?ts)))

(defquery unpaired-wind-speed []
  [?ws <- UnpairedWindSpeed])

(defquery cold-temp []
  [?c <- Cold])

(defquery hot-temp []
  [?h <- Hot])

(defquery temp-his []
  [?his <- TemperatureHistory])

(defquery temps-under-thresh []
  [?tut <- TempsUnderThreshold])
