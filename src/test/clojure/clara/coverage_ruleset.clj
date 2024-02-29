(ns clara.coverage-ruleset
  (:require [clara.rules :refer [defrule defquery insert!]]))

(defrule simple-rule-with-rhs
  [:weather [{:keys [temperature]}]
   (= temperature ?temperature)]
  =>
  (if (< ?temperature 50)
    (insert! {:type :climate :label "Cold"})
    (insert! {:type :climate :label "Warm"})))

(defquery climate-query
  []
  [?result <- :climate])
