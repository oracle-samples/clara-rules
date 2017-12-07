;;; Tests that clear-ns-productions! correction clears all vars marked as productions from the namespace.
#?(:clj
   (ns clara.test-clear-ns-productions
     (:require [clara.tools.testing-utils :refer [def-rules-test
                                                  side-effect-holder] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    query
                                    defrule
                                    defquery
                                    defsession
                                    clear-ns-productions!]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]))
   :cljs
   (ns clara.test-clear-ns-productions
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    query]]
               [cljs.test])
     (:require-macros [clara.rules :refer [defrule defquery defsession clear-ns-productions!]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(def before-clearing (atom nil))
(defrule rule-to-be-cleared
         [:a]
         =>
         (reset! before-clearing :before-clearing)
         (insert! :before-clearing))

(defquery query-to-be-cleared [] [?f <- :before-clearing])

#?(:clj
   (def ^:production-seq ns-production-seq-to-be-cleared
     [{:doc "Before clearing"
       :name "clara.test-clear-ns-productions/production-seq-to-be-cleared"
       :lhs '[{:type :b
               :constraints []}]
       :rhs '(clara.rules/insert! :before-clearing-seq)}]))

(defsession uncleared-session 'clara.test-clear-ns-productions :fact-type-fn identity)

(clear-ns-productions!)

(defrule rule-after-clearing
         [:a]
         =>
         (insert! :after-clearing))

(defquery query-before-clearing [] [?f <- :before-clearing])
(defquery query-after-clearing [] [?f <- :after-clearing])
(defquery query-before-clearing-seq [] [?f <- :before-clearing-seq])
(defquery query-after-clearing-seq [] [?f <- :after-clearing-seq])
#?(:clj
   (def ^:production-seq production-seq-after-clearing
     [{:doc "After clearing"
       :name "clara.test-clear-ns-productions/production-seq-after-clearing"
       :lhs '[{:type :b
               :constraints []}]
       :rhs '(clara.rules/insert! :after-clearing-seq)}]))

(defsession cleared-session 'clara.test-clear-ns-productions :fact-type-fn identity)

;;; Then tests validating what productions the respective sessions have.
(deftest cleared?
  (let [uncleared (-> uncleared-session (insert :a) (fire-rules))
        cleared (-> cleared-session (insert :a) (fire-rules))]
    (testing "cleared-session should not contain any productions before (clear-ns-productions!)"
      (is (= :before-clearing @before-clearing))
      (is (empty? (query cleared-session query-before-clearing)))
      (is (empty? (query cleared-session query-after-clearing)))
      (is (empty? (query cleared-session query-before-clearing-seq)))
      (is (empty? (query cleared-session query-after-clearing-seq))))))

