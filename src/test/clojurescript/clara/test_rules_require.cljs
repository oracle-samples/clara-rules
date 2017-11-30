(ns clara.test-rules-require
  (:require-macros [cljs.test :refer (is deftest run-tests testing)])
  (:require [cljs.test :as t]
            [clara.rules :refer [insert fire-rules query]
             :refer-macros [defsession]]
            [clara.rule-defs :as rd]
            [clara.rules.testfacts :as facts]))

;; Tests the case where rules/facts are required from a different namespace where the session is defined,
;; without an explicit :refer.
;; See https://github.com/cerner/clara-rules/issues/359

(defn- has-fact? [token fact]
       (some #{fact} (map first (:matches token))))

(defsession my-session 'clara.rule-defs)

(deftest test-simple-defrule
         (let [session (insert my-session (facts/->Temperature 10 "MCI"))]

              (fire-rules session)

              (is (has-fact? @rd/simple-defrule-side-effect (facts/->Temperature 10 "MCI")))
              (is (has-fact? @rd/other-defrule-side-effect (facts/->Temperature 10 "MCI")))))

(deftest test-simple-query
         (let [session (-> my-session
                           (insert (facts/->Temperature 15 "MCI"))
                           (insert (facts/->Temperature 10 "MCI"))
                           (insert (facts/->Temperature 80 "MCI"))
                           fire-rules)]

              ;; The query should identify all items that wer einserted and matchd the
              ;; expected criteria.
              (is (= #{{:?t 15} {:?t 10}}
                     (set (query session rd/cold-query))))))

(deftest test-simple-accumulator
         (let [session (-> my-session
                           (insert (facts/->Temperature 15 "MCI"))
                           (insert (facts/->Temperature 10 "MCI"))
                           (insert (facts/->Temperature 80 "MCI"))
                           fire-rules)]

              ;; Accumulator returns the lowest value.
              (is (= #{{:?t 10}}
                     (set (query session rd/coldest-query))))))

(deftest test-simple-insert
         (let [session (-> my-session
                           (insert (facts/->Temperature 15 "MCI"))
                           (insert (facts/->WindSpeed 45 "MCI"))
                           (fire-rules))]

              (is (= #{{:?fact (facts/->ColdAndWindy 15 45)}}
                     (set
                       (query session rd/find-cold-and-windy))))))

