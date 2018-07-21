(ns clara.long-running-tests
  (:require [clojure.test :refer :all]
            [clara.rules.compiler :as com]
            [schema.core :as sc]))

(deftest test-maximum-forms-per-eval
  ;; 5472 to ensure that we have enough forms to create a full batch, see clara.rules.compiler/forms-per-eval-default
  ;; or Issue 381 for more info
  (let [rules (for [_ (range 5472)
                    :let [fact-type (keyword (gensym))]]
                {:ns-name (ns-name *ns*)
                 :lhs [{:type fact-type
                        :constraints []}]
                 :rhs `(println ~(str fact-type))})
        rules-and-opts (conj (vector rules) :forms-per-eval 5472 :cache false)

        e (try
            ;; Not validating schema here because it will be very expensive
            (sc/without-fn-validation (com/mk-session rules-and-opts))
            (is false "Max batching size should have been exceeded, and exception thrown")
            (catch Exception e e))]

    ;; Validating that there were 5472 forms in the eval call.
    (is (= 5472 (count (-> e ex-data :compilation-ctxs))))
    (is (re-find #"method size exceeded"  (.getMessage e)))

    ;; Validate that the stated 5471 forms per eval will compile
    (is (sc/without-fn-validation (com/mk-session (conj (vector rules) :forms-per-eval 5471 :cache false))))))