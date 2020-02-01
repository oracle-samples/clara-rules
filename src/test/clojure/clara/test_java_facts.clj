(ns clara.test-java-facts
  (:require [clara.tools.testing-utils :as tu]
            [clara.rules :as rules]
            [clojure.test :refer [is deftest run-tests testing use-fixtures]])
  (:import [clara.test.facts
            BeanTestFact]))

;; A test to demonstrate that a Pojo with indexed property accessors can be used as an alpha root in a session,
;; see https://github.com/cerner/clara-rules/issues/446
(tu/def-rules-test test-basic-rule
  {:rules [kansas-rule [[[BeanTestFact
                          (= ?locs locations)
                          (some #(= "Kansas" %) ?locs)]]
                        (rules/insert! "Kansas Exists")]]
   :queries [string-query [[] [[?s <- String]]]]

   :sessions [empty-session [kansas-rule string-query] {}]}

  (let [locs (make-array String 2)]
    (aset locs 0 "Florida")
    (aset locs 1 "Kansas")
    (let [session-strings (into #{}
                                (map :?s)
                                (-> empty-session
                                    (rules/insert (BeanTestFact. locs))
                                    (rules/fire-rules)
                                    (rules/query string-query)))]
      (is (= 1 (count session-strings)))
      (is (contains? session-strings "Kansas Exists")))))