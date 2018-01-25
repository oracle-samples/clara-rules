(ns clara.test-java
  (:use clojure.test
        clara.rules.testfacts)
  (:require [clara.sample-ruleset :as sample]
            [clara.other-ruleset :as other])
  (:import [clara.rules.testfacts Temperature WindSpeed Cold ColdAndWindy LousyWeather First Second Third Fourth]
           [clara.rules QueryResult RuleLoader WorkingMemory]))

(defn- java-namespace-args 
  "The java API expects an arra of strings containing namespace names, so create that."
  []
  (doto (make-array String 2)
    (aset 0 "clara.sample-ruleset")
    (aset 1 "clara.other-ruleset")))

(deftest simple-rule

  (let [;; Simulate use of a typical Javaland object, the array list. 
        ;; Normally we'd just use the Clojure shorthand, but this is testing Java interop specifically
        facts (doto (java.util.ArrayList.)
                (.add (->Temperature 15 "MCI"))
                (.add (->Temperature 10 "BOS"))
                (.add (->Temperature 50 "SFO"))
                (.add (->Temperature -10 "CHI")))

        ;; Testing Java interop, so session is a clara.rules.WorkingMemory object.
        session (-> (RuleLoader/loadRules (java-namespace-args))            
                    (.insert facts)
                    (.fireRules))

        subzero-locs (.query session "clara.other-ruleset/subzero-locations" {})
        freezing-locs (.query session "clara.sample-ruleset/freezing-locations" {})]
    
    (is (= #{"CHI"}
           (set (map #(.getResult % "?loc") subzero-locs))))

    (is (= #{"CHI" "BOS" "MCI"}
           (set (map #(.getResult % "?loc") freezing-locs))))))

(deftest query-with-args
  (let [session
        (-> (RuleLoader/loadRules (java-namespace-args))
            (.insert [(->Temperature 15 "MCI") 
                      (->Temperature 10 "BOS") 
                      (->Temperature 50 "SFO") 
                      (->Temperature -10 "CHI")])
            (.fireRules))

        ;; Simulate invocation from Java by creating a hashmap of arguments.
        java-args (doto (java.util.HashMap.)
                    (.put "?loc" "CHI"))

        chicago-temp (.query session "clara.other-ruleset/temp-by-location" java-args)]
    
    (is (= #{-10}
           (set (map #(.getResult % "?temp") chicago-temp))))))
