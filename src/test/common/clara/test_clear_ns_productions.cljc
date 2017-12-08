;;; Tests that clear-ns-productions! correction clears all vars marked as productions from the namespace.
#?(:clj
   (ns clara.test-clear-ns-productions
     (:require [clara.tools.testing-utils :as tu]
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
               [cljs.test]
               [clara.tools.testing-utils :as tu])
     (:require-macros [clara.rules :refer [defrule defquery defsession clear-ns-productions!]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :each tu/side-effect-holder-fixture)

(defrule rule-to-be-cleared
         [:a]
         =>
         (reset! tu/side-effect-holder :before-clearing)
         (insert! :before-clearing))

(defquery query-to-be-cleared [] [?f <- :before-clearing])

#?(:clj
   (def ^:production-seq ns-production-seq-to-be-cleared
     [{:doc  "Before clearing"
       :name "clara.test-clear-ns-productions/production-seq-to-be-cleared"
       :lhs  '[{:type        :a
                :constraints []}]
       :rhs  '(clara.rules/insert! :before-clearing-seq)}]))

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
     [{:doc  "After clearing"
       :name "clara.test-clear-ns-productions/production-seq-after-clearing"
       :lhs  '[{:type        :a
                :constraints []}]
       :rhs  '(clara.rules/insert! :after-clearing-seq)}]))

(defsession cleared-session 'clara.test-clear-ns-productions :fact-type-fn identity)

;;; Then tests validating what productions the respective sessions have.
(deftest cleared?
  (let [uncleared (-> uncleared-session (insert :a) (fire-rules))]
    (is (= :before-clearing @tu/side-effect-holder))
    (reset! tu/side-effect-holder nil))
  (let [cleared (-> cleared-session (insert :a) (fire-rules))]
    (testing "cleared-session should not contain any productions before (clear-ns-productions!)"
      (is (= nil @tu/side-effect-holder))
      (is (empty? (query cleared query-before-clearing)))
      #?(:clj (is (not-empty (query cleared query-after-clearing)))))
    (is (empty? (query cleared query-before-clearing-seq)))
    #?(:clj (is (not-empty (query cleared query-after-clearing-seq))))))

(deftest query-cleared?
  (let [uncleared (-> uncleared-session (insert :a) (fire-rules))
        cleared (-> cleared-session (insert :a) (fire-rules))]
    (is (not-empty (query uncleared "clara.test-clear-ns-productions/query-to-be-cleared")))
    (is (thrown-with-msg? #?(:clj IllegalArgumentException :cljs js/Error) #"clara.test-clear-ns-productions/query-to-be-cleared"
                          (query cleared "clara.test-clear-ns-productions/query-to-be-cleared")))))