(ns clara.test-keyword-names
  (:require [clara.rules.testfacts :as facts]
            [clojure.test :refer [is deftest]]
            [clara.rules :refer [insert insert! fire-rules query mk-session]]
            [clara.rules.compiler :as com])
  (:import [clara.rules.testfacts Temperature WindSpeed ColdAndWindy]))

;;; Make explicit production data structures with keyword names.

(def is-cold-and-windy
  '{:ns-name clara.test-keyword-names,
    :lhs     [{:type        clara.rules.testfacts.Temperature,
               :constraints [(< temperature 20) (== ?t temperature)]}
              {:type        clara.rules.testfacts.WindSpeed,
               :constraints [(> windspeed 30) (== ?w windspeed)]}],
    :rhs     (do (insert! (clara.rules.testfacts/->ColdAndWindy ?t ?w))),
    :name    ::is-cold-and-windy,
    :doc     "Rule to determine whether it is indeed cold and windy."})


(def find-cold-and-windy
  '{:lhs    [{:type         clara.rules.testfacts.ColdAndWindy,
              :constraints  [],
              :fact-binding :?fact}],
    :params #{},
    :name   ::find-cold-and-windy})

;;; Test that we can properly assemble a session, insert facts, fire rules,
;;; and run a query with keyword-named productions.
(deftest test-simple-insert
  (let [session (-> (mk-session [is-cold-and-windy find-cold-and-windy])
                    (insert (facts/->Temperature 15 "MCI"))
                    (insert (facts/->WindSpeed 45 "MCI"))
                    (fire-rules))]

    (is (= [{:?fact (facts/->ColdAndWindy 15 45)}]
           (query session ::find-cold-and-windy)))))

;;; Verify that an exception is thrown when a duplicate name is encountered.
;;; Note we create the session with com/mk-session*, as com/mk-session allows
;;; duplicate names and will take what it considers to be the most recent
;;; definition of a production.
(deftest test-duplicate-name
  (try
    (com/mk-session* (com/add-production-load-order [is-cold-and-windy find-cold-and-windy find-cold-and-windy]) {})
    (catch Exception e
      (is (= (.getMessage e) "Non-unique production names: #{:clara.test-keyword-names/find-cold-and-windy}")))))