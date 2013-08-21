(ns clara.test-java
  (:use clojure.test
        clara.rules.testfacts)
  (:refer-clojure :exclude [==])
  (:require [clara.sample-ruleset :as sample]
            [clara.other-ruleset :as other])
  (import [clara.rules.testfacts Temperature WindSpeed Cold ColdAndWindy LousyWeather First Second Third Fourth]
          [clara.rules QueryResult RuleLoader WorkingMemory]))

(defn- java-namespace-args 
  "The java API expects an arra of strings containing namespace names, so create that."
  []
  (doto (make-array String 2)
    (aset 0 "clara.sample-ruleset")
    (aset 1 "clara.other-ruleset")))

(deftest simple-rule
  (let [session
        (-> (RuleLoader/loadRules (java-namespace-args))
            (.insert [(->Temperature 15 "MCI") 
                      (->Temperature 10 "BOS") 
                      (->Temperature 50 "SFO") 
                      (->Temperature -10 "CHI")])
            (.fireRules))
        subzero-locs (.query session "clara.other-ruleset/subzero-locations" {})
        freezing-locs (.query session "clara.sample-ruleset/freezing-locations" {})]
    
    (is (= #{"CHI"}
           (set (map #(.getResult % "?loc") subzero-locs))))

    (is (= #{"CHI" "BOS" "MCI"}
           (set (map #(.getResult % "?loc") freezing-locs))))))
