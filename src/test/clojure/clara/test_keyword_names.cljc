(ns clara.test-keyword-names
  #?(:cljs (:require-macros [clara.test-keyword-names]))
  (:require [clara.tools.testing-utils :as tu]
            [clara.rule-defs :as rd]
            [clara.rules.testfacts :as facts]
    #?(:clj  [clojure.test :refer [is deftest run-tests testing use-fixtures]])
    #?(:cljs [cljs.test :refer-macros [is deftest run-tests testing use-fixtures]])
    #?(:clj  [clara.rules :refer [insert insert! fire-rules query defsession]])
    #?(:cljs [clara.rules :refer [insert insert! fire-rules query] :refer-macros [defsession]])
    #?(:cljs [clara.macros :refer [defrule! defquery!]])
    )
  #?(:clj (:import [clara.rules.testfacts Temperature WindSpeed ColdAndWindy])))

#(:cljs
(defmacro dr [name body] (defrule! name body)))
#?(:clj
    (def is-cold-and-windy
        '{:ns-name clara.test-keyword-names,
    :lhs     [{:type        clara.rules.testfacts.Temperature,
               :constraints [(< temperature 20) (== ?t temperature)]}
              {:type        #?(:clj clara.rules.testfacts.WindSpeed :cljs clara.rules.testfacts/WindSpeed),
               :constraints [(> windspeed 30) (== ?w windspeed)]}],
    :rhs     (do (insert! (clara.rules.testfacts/->ColdAndWindy ?t ?w))),
    :name    ::is-cold-and-windy,
    :doc     "Rule to determine whether it is indeed cold and windy."})

    :cljs
    (dr is-cold-and-windy
    '{:ns-name clara.test-keyword-names,
    :lhs     [{:type        clara.rules.testfacts/Temperature,
               :constraints [(< temperature 20) (== ?t temperature)]}
              {:type        #?(:clj clara.rules.testfacts.WindSpeed :cljs clara.rules.testfacts/WindSpeed),
               :constraints [(> windspeed 30) (== ?w windspeed)]}],
    :rhs     (do (insert! (clara.rules.testfacts/->ColdAndWindy ?t ?w))),
    :name    ::is-cold-and-windy,
    :doc     "Rule to determine whether it is indeed cold and windy."}))


(#?(:clj def :cljs defquery!)
  find-cold-and-windy
  '{:lhs    [{:type         #?(:clj clara.rules.testfacts.ColdAndWindy :cljs clara.rules.testfacts/ColdAndWindy),
              :constraints  [],
              :fact-binding :?fact}],
    :params #{},
    :name   ::find-cold-and-windy})

(defsession my-session [is-cold-and-windy find-cold-and-windy])

(deftest test-simple-insert
  (let [session (-> my-session
                    (insert (facts/->Temperature 15 "MCI"))
                    (insert (facts/->WindSpeed 45 "MCI"))
                    (fire-rules))]

    (is (= #{{:?fact (facts/->ColdAndWindy 15 45)}}
           (set
             (query session ::find-cold-and-windy))))))

