(ns clara.test-rules-data
  (:require [clara.rules]
            [clara.rules.testfacts]))

(def the-rules
  [{:doc "Rule to determine whether it is indeed cold and windy."
    :name "clara.test-rules-data/is-cold-and-windy-data"
    :lhs '[{:type clara.rules.testfacts/Temperature
            :constraints [(< temperature 20)
                          (== ?t temperature)]}
           {:type clara.rules.testfacts/WindSpeed
            :constraints [(> windspeed 30)
                          (== ?w windspeed)]}]
    :rhs '(clara.rules/insert! (clara.rules.testfacts/->ColdAndWindy ?t ?w))}

   {:name "clara.test-rules-data/find-cold-and-windy-data"
    :lhs '[{:fact-binding :?fact
            :type clara.rules.testfacts/ColdAndWindy
            :constraints []}]
    :params #{}}])

(defn weather-rules
  "Return some weather rules"
  []
  the-rules)
