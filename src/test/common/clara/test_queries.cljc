#?(:clj
   (ns clara.test-queries
     (:require [clara.tools.testing-utils :refer [def-rules-test
                                                  side-effect-holder] :as tu]
               [clara.rules :refer [fire-rules
                                    insert
                                    query]]

               [clara.rules.testfacts :refer [->Temperature]]
               [clojure.test :refer [is deftest run-tests testing use-fixtures]]
               [schema.test :as st])
     (:import [clara.rules.testfacts
               Temperature]))

   :cljs
   (ns clara.test-queries
     (:require [clara.rules :refer [fire-rules
                                    insert
                                    query]]
               [clara.rules.testfacts :refer [->Temperature Temperature]]
               [cljs.test]
               [schema.test :as st])
     (:require-macros [clara.tools.testing-utils :refer [def-rules-test]]
                      [cljs.test :refer [is deftest run-tests testing use-fixtures]])))

(use-fixtures :once st/validate-schemas #?(:clj tu/opts-fixture))

(def-rules-test test-simple-query

  {:queries [cold-query [[]
                         [[Temperature (< temperature 20) (= ?t temperature)]]]]

   :sessions [empty-session [cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI"))
                    fire-rules)]

    ;; The query should identify all items that were inserted and matchd the
    ;; expected criteria.
    (is (= #{{:?t 15} {:?t 10}}
           (set (query session cold-query))))))

(def-rules-test test-param-query

  {:queries [cold-query [[:?l]
                         [[Temperature (< temperature 50)
                           (= ?t temperature)
                           (= ?l location)]]]]

   :sessions [empty-session [cold-query] {}]}

  (let [session (-> empty-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 20 "MCI")) ; Test multiple items in result.
                    (insert (->Temperature 10 "ORD"))
                    (insert (->Temperature 35 "BOS"))
                    (insert (->Temperature 80 "BOS"))
                    fire-rules)]

    ;; Query by location.
    (is (= #{{:?l "BOS" :?t 35}}
           (set (query session cold-query :?l "BOS"))))

    (is (= #{{:?l "MCI" :?t 15} {:?l "MCI" :?t 20}}
           (set (query session cold-query :?l "MCI"))))

    (is (= #{{:?l "ORD" :?t 10}}
           (set (query session cold-query :?l "ORD"))))))
      

