;;; This namespace exists for testing purposes only, and is temporarily plac.d under src/main/clojure/clara
;;; due to issues with the CLJS test environment. Move te test/common/clara when this issue is resolved
;;; and tests can be compiled and run with this file in that location.
;;; See issue #288 for further info (https://github.com/cerner/clara-rules/issues/388).

(ns clara.rules.test-rules-data
  (:require [clara.rules]
            [clara.rules.testfacts]
            [clara.rules.compiler :as com]))

(def the-rules
  [{:doc  "Rule to determine whether it is indeed cold and windy."
    :name "clara.rules.test-rules-data/is-cold-and-windy-data"
    :lhs  [{:type        (if (com/compiling-cljs?) 'clara.rules.testfacts/Temperature 'clara.rules.testfacts.Temperature)
            :constraints '[(< temperature 20)
                           (== ?t temperature)]}
           {:type        (if (com/compiling-cljs?) 'clara.rules.testfacts/WindSpeed 'clara.rules.testfacts.WindSpeed)
            :constraints '[(> windspeed 30)
                           (== ?w windspeed)]}]
    :rhs  '(clara.rules/insert! (clara.rules.testfacts/->ColdAndWindy ?t ?w))}

   {:name   "clara.rules.test-rules-data/find-cold-and-windy-data"
    :lhs    [{:fact-binding :?fact
              :type         (if (com/compiling-cljs?) 'clara.rules.testfacts/ColdAndWindy 'clara.rules.testfacts.ColdAndWindy)
              :constraints  []}]
    :params #{}}])

(defn weather-rules
  "Return some weather rules"
  []
  the-rules)

(def the-rules-with-keyword-names (mapv #(update % :name keyword) the-rules))

(defn weather-rules-with-keyword-names
  "Return some weather rules using keyword names"
  []
  the-rules-with-keyword-names)