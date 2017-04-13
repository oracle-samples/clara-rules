(ns clara.tools.test-fact-graph
  (:require [clara.tools.fact-graph :as g]
            [clara.rules.dsl :as dsl]
            [clara.rules.testfacts :as tf]
            [clara.rules :as cr]
            [clojure.test :refer :all]
            [clara.rules.accumulators :as acc]
            [clojure.walk :as w]
            [schema.test :as st])
  (:import [clara.rules.testfacts
            Cold
            WindSpeed
            ColdAndWindy]))

(use-fixtures :once st/validate-schemas)

(deftest test-basic-fact-graph
  (let [rule (assoc
              (dsl/parse-rule [[Cold (= ?t temperature) (< ?t 0)]
                              [WindSpeed (= ?w windspeed) (> ?w 30)]]
                              (cr/insert! (tf/->ColdAndWindy ?t ?w)))
              :name "cold-windy-rule")

        empty-session (cr/mk-session [rule] :cache false)

        actual-graph (-> empty-session
                         (cr/insert (tf/->Cold -10)
                                    (tf/->WindSpeed 50 "MCI"))
                         cr/fire-rules
                         g/session->fact-graph)

        expected-graph (let [act-node (g/map->RuleActivationNode {:rule-name "cold-windy-rule"
                                                                  :id 1})]
                         {:forward-edges
                          {(tf/->Cold -10) #{act-node}
                           (tf/->WindSpeed 50 "MCI") #{act-node}
                           act-node #{(tf/->ColdAndWindy -10 50)}}
                          :backward-edges
                          {act-node #{(tf/->Cold -10)
                                      (tf/->WindSpeed 50 "MCI")}
                           (tf/->ColdAndWindy -10 50) #{act-node}}})]

    (is (= expected-graph actual-graph))))

(deftest test-accum-rule-fact-graph
  (let [rule  (assoc
               (dsl/parse-rule [[?t <- (acc/min :temperature) :from [Cold]]
                                [WindSpeed (= ?w windspeed) (> ?w 30)]]
                               (cr/insert! (tf/->ColdAndWindy ?t ?w)))
               :name "cold-windy-rule")

        empty-session (cr/mk-session [rule] :cache false)

        actual-graph (-> empty-session
                         (cr/insert (tf/->Cold -10)
                                    (tf/->Cold 10)
                                    (tf/->WindSpeed 50 "MCI"))
                         cr/fire-rules
                         g/session->fact-graph)

        expected-graph (let [act-node (g/map->RuleActivationNode {:rule-name "cold-windy-rule"
                                                                  :id 1})
                             accum-node (g/map->AccumulationNode {:id 2})
                             accum-result-node (g/map->AccumulationResultNode {:id 3
                                                                               :result -10})]
                         {:forward-edges
                          {(tf/->Cold -10) #{accum-node}
                           (tf/->Cold 10) #{accum-node}
                           accum-node #{accum-result-node}
                           accum-result-node #{act-node}
                           (tf/->WindSpeed 50 "MCI") #{act-node}
                           act-node #{(tf/->ColdAndWindy -10 50)}}
                          :backward-edges
                          {accum-node #{(tf/->Cold -10) (tf/->Cold 10)}
                           accum-result-node #{accum-node}
                           act-node #{(tf/->WindSpeed 50 "MCI")
                                      accum-result-node}
                           (tf/->ColdAndWindy -10 50) #{act-node}}})]

    (is (= expected-graph actual-graph))))

(deftest test-fact-duplicates
  (let [rule1 (assoc (dsl/parse-rule [[?a <- "a"]]
                                     (cr/insert! "b"))
                     :name "b-rule")
        rule2 (assoc (dsl/parse-rule [[?b <- "b"]]
                                     (cr/insert! "c"))
                     :name "c-rule")

        empty-session (cr/mk-session [rule1 rule2]
                                     :cache false
                                     :fact-type-fn identity)

        actual-graph (-> empty-session
                         (cr/insert "a" "a")
                         cr/fire-rules
                         g/session->fact-graph)

        expected-graph (let [rule1-node1 (g/map->RuleActivationNode {:rule-name "b-rule"
                                                                     :id 1})
                             rule1-node2 (g/map->RuleActivationNode {:rule-name "b-rule"
                                                                     :id 2})
                             rule2-node1 (g/map->RuleActivationNode {:rule-name "c-rule"
                                                                     :id 3})
                             rule2-node2 (g/map->RuleActivationNode {:rule-name "c-rule"
                                                                     :id 4})]
                         {:forward-edges {"a" #{rule1-node1 rule1-node2}
                                          rule1-node1 #{"b"}
                                          rule1-node2 #{"b"}
                                          "b" #{rule2-node1 rule2-node2}
                                          rule2-node1 #{"c"}
                                          rule2-node2 #{"c"}}
                          :backward-edges {rule1-node1 #{"a"}
                                           rule1-node2 #{"a"}
                                           "b" #{rule1-node1 rule1-node2}
                                           rule2-node1 #{"b"}
                                           rule2-node2 #{"b"}
                                           "c" #{rule2-node1 rule2-node2}}})]

    (is (= actual-graph expected-graph))))

(deftest test-duplicate-fact-from-different-rules
  (let [rule1 (assoc (dsl/parse-rule [[?a <- "a"]]
                                     (cr/insert! "c")
                                     {:salience 1})
                     :name "a-rule")
        rule2 (assoc (dsl/parse-rule [[?b <- "b"]]
                                     (cr/insert! "c"))
                     :name "b-rule")

        empty-session (cr/mk-session [rule1 rule2]
                                     :cache false
                                     :fact-type-fn identity)

        actual-graph (-> empty-session
                         (cr/insert "a" "b")
                         cr/fire-rules
                         g/session->fact-graph)

        expected-graph (let [rule1-node (g/map->RuleActivationNode {:rule-name "a-rule"
                                                                    :id 1})
                             rule2-node (g/map->RuleActivationNode {:rule-name "b-rule"
                                                                    :id 2})]

                         {:forward-edges {"a" #{rule1-node}
                                          "b" #{rule2-node}
                                          rule1-node #{"c"}
                                          rule2-node #{"c"}}
                          :backward-edges {rule1-node #{"a"}
                                           rule2-node #{"b"}
                                           "c" #{rule1-node rule2-node}}})]

    (is (= expected-graph actual-graph))))
