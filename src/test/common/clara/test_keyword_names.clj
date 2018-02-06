(ns clara.test-keyword-names
  (:require [clojure.test :refer :all]
            [clara.rules :refer :all]
            [clara.rules.accumulators :as acc]
            [clara.rules.testfacts :as tf])
  (:import [clara.rules.testfacts Temperature WindSpeed ColdAndWindy]))

(def is-cold-and-windy
  '{:ns-name clara.test-keyword-names,
    :lhs     [{:type        clara.rules.testfacts.Temperature,
               :constraints [(< temperature 20) (== ?t temperature)]}
              {:type        clara.rules.testfacts.WindSpeed,
               :constraints [(> windspeed 30) (== ?w windspeed)]}],
    :rhs     (do (insert! (tf/->ColdAndWindy ?t ?w))),
    :name    ::is-cold-and-windy,
    :doc     "Rule to determine whether it is indeed cold and windy."})

(def find-cold-and-windy
  '{:lhs    [{:type         clara.rules.testfacts.ColdAndWindy,
              :constraints  [],
              :fact-binding :?fact}],
    :params #{},
    :name   ::find-cold-and-windy})

(defsession my-session [is-cold-and-windy find-cold-and-windy])

(deftest foo
  (binding [*out* *err*]
    (clojure.pprint/pprint is-cold-and-windy)
    (clojure.pprint/pprint find-cold-and-windy)))
