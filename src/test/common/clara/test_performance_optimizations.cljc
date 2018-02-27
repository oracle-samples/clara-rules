;; These tests validate that operations that the rules engine should optimize
;; away are in fact optimized away.  The target here is not the actual execution time,
;; which will vary per system, but verification that the action operations in question are not performed.
#?(:clj
   (ns clara.test-performance-optimizations
     (:require [clara.tools.testing-utils :refer [def-rules-test
                                                  side-effect-holder] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    query]]

               [clara.rules.testfacts :refer [->Cold ->ColdAndWindy]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [clara.rules.accumulators]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Cold
               ColdAndWindy]))

   :cljs
   (ns clara.test-performance-optimizations
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    insert!
                                    query]]
               [clara.rules.testfacts :refer [->Cold Cold
                                              ->ColdAndWindy ColdAndWindy]]
               [clara.rules.accumulators]
               [cljs.test]
               [schema.test :as st]
               [clara.tools.testing-utils :refer [side-effect-holder] :as tu])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))
(use-fixtures :each tu/side-effect-holder-fixture)

;; Note that if the binding is present, this will show up as an error (since an exception is thrown)
;; rather than a failure.  This is not ideal but since this will happen at compile time in ClojureScript,
;; rather than test execution time, we can't just register a test failure via the clojure.test/is macro.
#?(:clj
   (defmacro fail-if-binding-present
     []
     (when (contains? &env '?unused-binding)
       (throw (ex-info "Binding that is not expected to be present in the let-bindings of a RHS is present"
                       {:environment &env})))))

;; See issue https://github.com/cerner/clara-rules/issues/383
;; This validates that we don't create let bindings for binding
;; variables that aren't used.  Doing so both imposes runtime costs
;; and increases the size of the generated code that must be evaluated.
(def-rules-test test-unused-rhs-binding-not-bound

  {:rules [cold-windy-rule [[[ColdAndWindy (= ?used-binding temperature) (= ?unused-binding windspeed)]]
                            (do
                              (fail-if-binding-present)
                              (insert! (->Cold ?used-binding)))]]

   :queries [cold-query [[] [[Cold (= ?c temperature)]]]]

   :sessions [empty-session [cold-windy-rule cold-query] {}]}

  (is (= [{:?c 0}]
         (-> empty-session
             (insert (->ColdAndWindy 0 0))
             fire-rules
             (query cold-query)))))
