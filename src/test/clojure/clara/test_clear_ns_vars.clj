;;; Tests that clear-ns-vars! correction clears all vars marked as productions from the namespace.
(ns clara.test-clear-ns-vars
  (:require
   [clara.rules :refer [clear-ns-vars! defquery defrule defsession
                        fire-rules insert insert! query]]
   [clara.tools.testing-utils :as tu]
   [clojure.test :refer [deftest is testing use-fixtures]])
  (:import
   [java.lang IllegalArgumentException]))

(use-fixtures :each tu/side-effect-holder-fixture)

(defrule rule-to-be-cleared
  [:a]
  =>
  (reset! tu/side-effect-holder :before-clearing)
  (insert! :before-clearing))

(defquery query-to-be-cleared [] [?f <- :before-clearing])

(def ^:production-seq ns-production-seq-to-be-cleared
  [{:doc  "Before clearing"
    :name "clara.test-clear-ns-vars/production-seq-to-be-cleared"
    :lhs  '[{:type        :a
             :constraints []}]
    :rhs  '(clara.rules/insert! :before-clearing-seq)}])

(defsession uncleared-session 'clara.test-clear-ns-vars :fact-type-fn identity)

(clear-ns-vars!)

(defrule rule-after-clearing
  [:a]
  =>
  (insert! :after-clearing))

(defquery query-before-clearing [] [?f <- :before-clearing])
(defquery query-after-clearing [] [?f <- :after-clearing])
(defquery query-before-clearing-seq [] [?f <- :before-clearing-seq])
(defquery query-after-clearing-seq [] [?f <- :after-clearing-seq])

(def ^:production-seq production-seq-after-clearing
  [{:doc  "After clearing"
    :name "clara.test-clear-ns-vars/production-seq-after-clearing"
    :lhs  '[{:type        :a
             :constraints []}]
    :rhs  '(clara.rules/insert! :after-clearing-seq)}])

(defsession cleared-session 'clara.test-clear-ns-vars :fact-type-fn identity)

;;; Then tests validating what productions the respective sessions have.
(deftest cleared?
  (let [uncleared (-> uncleared-session (insert :a) (fire-rules))]
    (is (= :before-clearing @tu/side-effect-holder))
    (reset! tu/side-effect-holder nil))
  (let [cleared (-> cleared-session (insert :a) (fire-rules))]
    (testing "cleared-session should not contain any productions before (clear-ns-vars!)"
      (is (= nil @tu/side-effect-holder))
      (is (empty? (query cleared query-before-clearing)))
      (is (not-empty (query cleared query-after-clearing))))
    (is (empty? (query cleared query-before-clearing-seq)))
    (is (not-empty (query cleared query-after-clearing-seq)))))

(deftest query-cleared?
  (let [uncleared (-> uncleared-session (insert :a) (fire-rules))
        cleared (-> cleared-session (insert :a) (fire-rules))]
    (is (not-empty (query uncleared "clara.test-clear-ns-vars/query-to-be-cleared")))
    (is (thrown-with-msg? IllegalArgumentException #"clara.test-clear-ns-vars/query-to-be-cleared"
                          (query cleared "clara.test-clear-ns-vars/query-to-be-cleared")))))
