(ns clara.tools.test-inspect
  (:require [clara.tools.inspect :refer :all]
            [clara.rules :refer :all]
            [clara.rules.testfacts :refer :all]
            [clara.rules.dsl :as dsl]
            [clara.rules.engine :as eng]
            [clojure.test :refer :all])
  (:import [clara.rules.testfacts Temperature WindSpeed Cold 
            ColdAndWindy LousyWeather First Second Third Fourth]))



(deftest test-simple-inspect

  ;; Create some rules and a session for our test.
  (let [cold-query (assoc (dsl/parse-query [] [[Temperature (< temperature 20) (= ?t temperature)]])
                     :name "Beep")
        
        cold-rule (dsl/parse-rule [[Temperature (< temperature 20) (= ?t temperature)]] 
                                  (println ?t "is cold!") )

        hot-rule (dsl/parse-rule [[Temperature (> temperature 80) (= ?t temperature)]] 
                                  (println ?t "is hot!") )

        session (-> (mk-session [cold-query cold-rule hot-rule]) 
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 90 "MCI")))

        rule-dump (inspect session)]

    ;; Retrieve the tokens matching the cold query.
    (is (= [(eng/map->Token {:facts [(->Temperature 15 "MCI")], :bindings {:?t 15}})
            (eng/map->Token {:facts [(->Temperature 10 "MCI")], :bindings {:?t 10}})]           
         (get-in rule-dump [:query-matches cold-query])))

    ;; Retrieve tokens matching the hot rule.
    (is (= [(eng/map->Token {:facts [(->Temperature 90 "MCI")], :bindings {:?t 90}})]           
         (get-in rule-dump [:rule-matches hot-rule])))
    
    ;; Ensure the first condition in the rule matches the expected facts.
    (is (= [(->Temperature 15 "MCI") (->Temperature 10 "MCI")]
           (get-in rule-dump [:condition-matches  (first (:lhs cold-rule))])))))
