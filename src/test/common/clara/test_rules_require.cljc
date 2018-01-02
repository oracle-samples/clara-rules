(ns clara.test-rules-require
  (:require [clara.tools.testing-utils :as tu]
            [clara.rule-defs :as rd]
            [clara.rules.testfacts :as facts]
    #?(:clj  [clojure.test :refer [is deftest run-tests testing use-fixtures]])
    #?(:cljs [cljs.test :refer-macros [is deftest run-tests testing use-fixtures]])
    #?(:clj  [clara.rules :refer [insert fire-rules query defsession]])
    #?(:cljs [clara.rules :refer [insert fire-rules query] :refer-macros [defsession]])))

;; Tests the case where rules/facts are required from a different namespace where the session is defined,
;; without an explicit :refer.
;; See https://github.com/cerner/clara-rules/issues/359

(use-fixtures :each tu/side-effect-holder-fixture)

(defsession my-session 'clara.rule-defs)

(deftest test-simple-defrule
  (let [session (insert my-session (facts/->Temperature 10 "MCI"))]
    (fire-rules session)
    (is (= @tu/side-effect-holder (facts/->Temperature 10 "MCI")))))

(deftest test-simple-query
  (let [session (-> my-session
                    (insert (facts/->Temperature 15 "MCI"))
                    (insert (facts/->Temperature 10 "MCI"))
                    (insert (facts/->Temperature 80 "MCI"))
                    fire-rules)]

    ;; The query should identify all items that were inserted and matched the
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

