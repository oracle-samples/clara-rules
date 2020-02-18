(ns clara.test-java-facts
  (:require [clara.tools.testing-utils :as tu]
            [clara.rules :as rules]
            [clojure.test :refer [is deftest run-tests testing use-fixtures]]
            [clara.rules.compiler :as com])
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

  (let [locs (doto (make-array String 2)
               (aset 0 "Florida")
               (aset 1 "Kansas"))]
    (let [session-strings (map :?s
                               (-> empty-session
                                   (rules/insert (BeanTestFact. locs))
                                   (rules/fire-rules)
                                   (rules/query string-query)))]
      (is (= ["Kansas Exists"] session-strings)))))

;; Using an indexed property accessor that doesn't have a standard accessor will throw exception as clara cannot resolve
;; the usage of the accessor. See https://github.com/cerner/clara-rules/issues/446
(deftest test-indexed-property-accessor
  (let [rule-using-unsupported-accessor {:ns-name (ns-name *ns*)
                                         :lhs [{:type BeanTestFact :constraints [`(contains? ~'roadConditions "Slippery")]}]
                                         :rhs `(rules/insert! "doesn't matter")
                                         :name "rule-using-unsupported-accessor"}]
    (try
      (com/mk-session [[rule-using-unsupported-accessor]])
      (is false "An exception should be thrown")
      (catch Exception e
        (loop [exc e]
          (cond
            (re-find #"Failed compiling alpha node" (.getMessage exc))
            :success

            (.getCause exc)
            (recur (.getCause exc))

            :else
            (is false "Exception didn't contain a message containing alpha node failure")))))))