(ns clara.performance.test-compilation
  (:require [clojure.test :refer :all]
            [clara.rules.compiler :as com]
            [clara.rules :as r]
            [clara.rules.durability :as dura]
            [clara.rules.durability.fressian :as fres]
            [clara.rules.accumulators :as acc]
            [clara.tools.testing-utils :as utils])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]))

(defn filter-fn
  [seed-keyword]
  (= seed-keyword (keyword (gensym))))

(def base-production
  {:ns-name (symbol (str *ns*))})

(defn generate-filter-productions
  [seed-syms]
  (into {}
        (for [seed-sym seed-syms
              :let [next-fact (symbol (str seed-sym "prime"))
                    production (assoc base-production
                                 :lhs [{:type (keyword seed-sym)
                                        :constraints [`(= ~'this ~'?binding) `(filter-fn ~'this)]}]
                                 :rhs `(r/insert! (with-meta ~(set (repeatedly 10 #(rand-nth (range 100))))
                                                             {:type ~(keyword next-fact)
                                                              :val ~'?binding})))]]
          [next-fact production])))

(defn generate-compose-productions
  [seed-syms]
  (let [template (fn template
                   ([l] (template l l))
                   ([l r] (let [next-fact (symbol (str l r "prime"))
                                production (assoc base-production
                                             :lhs [{:type (keyword l)
                                                    :constraints [`(= ~'this ~'?binding-l)]}
                                                   {:type (keyword r)
                                                    :constraints [`(= ~'?binding-l ~'this)]}]
                                             :rhs `(r/insert! (with-meta ~(set (repeatedly 10 #(rand-nth (range 100))))
                                                                         {:type ~(keyword next-fact)})))]
                            [next-fact production])))]
    (into {}
          (for [combo (partition-all 2 (shuffle seed-syms))]
            (apply template combo)))))

(defn generate-collection-productions
  [seed-syms]
  (into {}
        (for [seed-sym seed-syms
              :let [next-fact (symbol (str seed-sym "prime"))
                    production (assoc base-production
                                 :lhs [{:accumulator `(acc/all)
                                        :from {:type (keyword seed-sym),
                                               :constraints [`(filter-fn ~'this)]}
                                        :result-binding :?binding}]
                                 :rhs `(r/insert! (with-meta ~'?binding
                                                             {:type ~(keyword next-fact)})))]]
          [next-fact production])))

(defn generate-queries
  [seed-syms]
  (into {}
        (for [seed-sym seed-syms
              :let [production (assoc base-production
                                 :lhs [{:type (keyword seed-sym)
                                        :constraints []
                                        :fact-binding :?binding}]
                                 :params #{})]]
          [seed-sym production])))

(defn generate-rules-and-opts
  [num-starter-facts]
  (let [starter-seeds (repeatedly num-starter-facts gensym)]
    (-> (reduce (fn [[seeds prods] next-fn]
                  (let [new-productions-and-facts (next-fn seeds)]
                    [(keys new-productions-and-facts)
                     (concat prods
                             (vals new-productions-and-facts))]))
                [starter-seeds []]
                [generate-filter-productions
                 generate-compose-productions
                 generate-collection-productions
                 generate-queries])
        second
        vector
        (conj :cache false))))

(deftest compilation-performance-test
  (let [rules (generate-rules-and-opts 500)]
    (testing "Session creation performance"
      (utils/run-performance-test
        {:description "Generated Session Compilation"
         :func #(com/mk-session rules)
         :iterations 50
         :mean-assertion (partial > 5000)}))

    (let [session (com/mk-session rules)
          os (ByteArrayOutputStream.)]
      (testing "Session rulebase serialization performance"
        (utils/run-performance-test
          {:description "Session rulebase serialization"
           :func #(dura/serialize-rulebase
                    session
                    (fres/create-session-serializer (ByteArrayOutputStream.)))
           :iterations 50
           :mean-assertion (partial > 1000)}))

      (testing "Session rulebase deserialization performance"
        (dura/serialize-rulebase
          session
          (fres/create-session-serializer os))

        (let [session-bytes (.toByteArray os)]
          (utils/run-performance-test
            {:description "Session rulebase deserialization"
             :func #(dura/deserialize-rulebase
                      (fres/create-session-serializer (ByteArrayInputStream. session-bytes)))
             :iterations 50
             :mean-assertion (partial > 5000)}))))))
