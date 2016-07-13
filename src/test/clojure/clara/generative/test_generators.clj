(ns clara.generative.test-generators
  (:require
   [clara.generative.generators :refer :all]
   [clojure.test :refer :all]
   [schema.test]))

(use-fixtures :once schema.test/validate-schemas)

;; Basic sanity test of the insert/retract/fire permutation generation.
(deftest test-basic-permutations
  (let [base-ops [{:type :insert
                   :facts [:a]}]
        permuted-ops (ops->permutations base-ops {:dup-level 1})]
    (is (= (set permuted-ops)
           #{[{:type :insert, :facts [:a]}]
             [{:type :insert, :facts [:a]}
              {:type :insert, :facts [:a]}
              {:type :retract, :facts [:a]}]
             [{:type :insert, :facts [:a]}
              {:type :retract, :facts [:a]}
              {:type :insert, :facts [:a]}]}))))
