(ns clara.tools.test-fact-graph
  (:require [clara.tools.testing-utils :as tu]
            [clara.tools.fact-graph :as g]
            [clara.rules :as cr]
            [clara.rules.accumulators :as acc]
            [schema.test :as st]
    #?(:clj [clojure.test :refer [is deftest run-tests testing use-fixtures]]
       :cljs [cljs.test :refer-macros [is deftest run-tests testing use-fixtures]])
    #?(:clj [clara.rules.testfacts :as tf]
       :cljs [clara.rules.testfacts :refer [Cold WindSpeed ColdAndWindy] :as tf]))
  #?(:clj
     (:import [clara.rules.testfacts Cold WindSpeed ColdAndWindy])))

(use-fixtures :once st/validate-schemas)

(tu/def-rules-test test-basic-fact-graph
  {:rules    [cold-windy-rule [[[Cold (= ?t temperature) (< ?t 0)]
                                [WindSpeed (= ?w windspeed) (> ?w 30)]]
                               (cr/insert! (tf/->ColdAndWindy ?t ?w))]]
   :sessions [empty-session [cold-windy-rule] {:cache false}]}
  (let [actual-graph (-> empty-session
                         (cr/insert (tf/->Cold -10)
                                    (tf/->WindSpeed 50 "MCI"))
                         cr/fire-rules
                         g/session->fact-graph)

        expected-graph (let [act-node (g/map->RuleActivationNode {:rule-name "cold-windy-rule"
                                                                  :id        1})]
                         {:forward-edges
                          {(tf/->Cold -10)           #{act-node}
                           (tf/->WindSpeed 50 "MCI") #{act-node}
                           act-node                  #{(tf/->ColdAndWindy -10 50)}}
                          :backward-edges
                          {act-node                   #{(tf/->Cold -10)
                                                        (tf/->WindSpeed 50 "MCI")}
                           (tf/->ColdAndWindy -10 50) #{act-node}}})]

    (is (= expected-graph actual-graph))))

(tu/def-rules-test test-accum-rule-fact-graph
  {:rules    [cold-windy-rule [[[?t <- (acc/min :temperature) :from [Cold]]
                                [WindSpeed (= ?w windspeed) (> ?w 30)]]
                               (cr/insert! (tf/->ColdAndWindy ?t ?w))]]
   :sessions [empty-session [cold-windy-rule] {:cache false}]}
  (let [actual-graph (-> empty-session
                         (cr/insert (tf/->Cold -10)
                                    (tf/->Cold 10)
                                    (tf/->WindSpeed 50 "MCI"))
                         cr/fire-rules
                         g/session->fact-graph)

        expected-graph (let [act-node (g/map->RuleActivationNode {:rule-name "cold-windy-rule"
                                                                  :id        1})
                             accum-node (g/map->AccumulationNode {:id 2})
                             accum-result-node (g/map->AccumulationResultNode {:id     3
                                                                               :result -10})]
                         {:forward-edges
                          {(tf/->Cold -10)           #{accum-node}
                           (tf/->Cold 10)            #{accum-node}
                           accum-node                #{accum-result-node}
                           accum-result-node         #{act-node}
                           (tf/->WindSpeed 50 "MCI") #{act-node}
                           act-node                  #{(tf/->ColdAndWindy -10 50)}}
                          :backward-edges
                          {accum-node                 #{(tf/->Cold -10) (tf/->Cold 10)}
                           accum-result-node          #{accum-node}
                           act-node                   #{(tf/->WindSpeed 50 "MCI")
                                                        accum-result-node}
                           (tf/->ColdAndWindy -10 50) #{act-node}}})]

    (is (= expected-graph actual-graph))))

(tu/def-rules-test test-fact-duplicates
  {:rules    [b-rule [[[?a <- "a"]]
                      (cr/insert! "b")]
              c-rule [[[?b <- "b"]]
                      (cr/insert! "c")]]
   :sessions [empty-session [b-rule c-rule] {:cache false :fact-type-fn identity}]}
  (let [actual-graph (-> empty-session
                         (cr/insert "a" "a")
                         cr/fire-rules
                         g/session->fact-graph)

        expected-graph (let [rule1-node1 (g/map->RuleActivationNode {:rule-name "b-rule"
                                                                     :id        1})
                             rule1-node2 (g/map->RuleActivationNode {:rule-name "b-rule"
                                                                     :id        2})
                             rule2-node1 (g/map->RuleActivationNode {:rule-name "c-rule"
                                                                     :id        3})
                             rule2-node2 (g/map->RuleActivationNode {:rule-name "c-rule"
                                                                     :id        4})]
                         {:forward-edges  {"a"         #{rule1-node1 rule1-node2}
                                           rule1-node1 #{"b"}
                                           rule1-node2 #{"b"}
                                           "b"         #{rule2-node1 rule2-node2}
                                           rule2-node1 #{"c"}
                                           rule2-node2 #{"c"}}
                          :backward-edges {rule1-node1 #{"a"}
                                           rule1-node2 #{"a"}
                                           "b"         #{rule1-node1 rule1-node2}
                                           rule2-node1 #{"b"}
                                           rule2-node2 #{"b"}
                                           "c"         #{rule2-node1 rule2-node2}}})]

    (is (= actual-graph expected-graph))))

(tu/def-rules-test test-duplicate-fact-from-different-rules
  {:rules    [a-rule [[[?a <- "a"]]
                      (cr/insert! "c")
                      {:salience 1}]
              b-rule [[[?b <- "b"]]
                      (cr/insert! "c")]]
   :sessions [empty-session [a-rule b-rule] {:cache false :fact-type-fn identity}]}
  (let [actual-graph (-> empty-session
                         (cr/insert "a" "b")
                         cr/fire-rules
                         g/session->fact-graph)

        expected-graph (let [rule1-node (g/map->RuleActivationNode {:rule-name "a-rule"
                                                                    :id        1})
                             rule2-node (g/map->RuleActivationNode {:rule-name "b-rule"
                                                                    :id        2})]

                         {:forward-edges  {"a"        #{rule1-node}
                                           "b"        #{rule2-node}
                                           rule1-node #{"c"}
                                           rule2-node #{"c"}}
                          :backward-edges {rule1-node #{"a"}
                                           rule2-node #{"b"}
                                           "c"        #{rule1-node rule2-node}}})]

    (is (= expected-graph actual-graph))))
