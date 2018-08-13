(ns clara.performance.test-compilation
  (:require [clojure.test :refer :all]
            [clara.rules.compiler :as com]
            [clara.rules :as r]
            [clara.rules.durability :as dura]
            [clara.rules.durability.fressian :as fres]
            [clara.rules.accumulators :as acc])
  (:import [java.io ByteArrayOutputStream ByteArrayInputStream]))

(defn filter-fn
  [seed-keyword]
  (= seed-keyword (keyword (gensym))))

(defn time-execution
  [func]
  (let [start (System/currentTimeMillis)
        _ (func)
        stop (System/currentTimeMillis)]
    (- stop start)))

(defn execute-tests
  [func iterations]
  (let [execution-times (for [_ (range iterations)]
                          (time-execution func))
        sum #(reduce + %)
        mean (/ (sum execution-times) iterations)
        std (->
              (into []
                    (comp
                      (map #(- % mean))
                      (map #(Math/pow (double %) 2.0)))
                    execution-times)
              sum
              (/ iterations)
              Math/sqrt)]
    {:std (double std)
     :mean (double mean)}))

(defn run-performance-test
  "Created as a rudimentary alternative to criterium, due to assumptions made during benchmarking. Specifically, that
   criterium attempts to reach a steady state of compiled and loaded classes. This fundamentally doesn't work when the
   metrics needed rely on compilation or evaluation."
  [form]
  (let [{:keys [description func iterations mean-assertion]} form
        {:keys [std mean]} (execute-tests func iterations)]
    (println (str \newline "Running Performance tests for:"))
    (println description)
    (println "==========================================")
    (println (str "Mean: " mean "ms"))
    (println (str "Standard Deviation: " std "ms" \newline))
    (is (mean-assertion mean)
        (str "Actual mean value: " mean))))

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
      (run-performance-test
        {:description "Generated Session Compilation"
         :func #(com/mk-session rules)
         :iterations 50
         :mean-assertion (partial > 5000)}))

    (let [session (com/mk-session rules)
          os (ByteArrayOutputStream.)]
      (testing "Session rulebase serialization performance"
        (run-performance-test
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
          (run-performance-test
            {:description "Session rulebase deserialization"
             :func #(dura/deserialize-rulebase
                      (fres/create-session-serializer (ByteArrayInputStream. session-bytes)))
             :iterations 50
             :mean-assertion (partial > 5000)}))))))
