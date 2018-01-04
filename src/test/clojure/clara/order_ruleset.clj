(ns clara.order-ruleset
  (:use clara.rules
        clara.rules.testfacts)
  (:refer-clojure :exclude [==])
  (:import [clara.rules.testfacts
            Temperature
            WindSpeed
            Cold
            ColdAndWindy
            LousyWeather]))

(def ^:dynamic *rule-order-atom* nil)

(def ^{:dynamic true
       :production-seq true}
  *rule-seq-prior* [])

(defrule rule-C
  [Cold (constantly true)]
  =>
  (swap! *rule-order-atom* conj :C))

(defrule rule-D
  [Cold (constantly true)]
  =>
  (swap! *rule-order-atom* conj :D))

(def ^{:dynamic true
       :production-seq true}
  *rule-seq-after* [])
