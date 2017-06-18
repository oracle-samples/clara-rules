(ns clara.test
  (:require-macros [cljs.test :as test])
  (:require [clara.test-rules]
            [cljs.test]
            [clara.test-salience]
            [clara.test-complex-negation]
            [clara.test-common]
            [clara.test-testing-utils]
            [clara.test-accumulators]))

(enable-console-print!)

(defmethod cljs.test/report [:cljs.test/default :end-run-tests] [m]
  (if (cljs.test/successful? m)
    (println "Success!")
    (println "FAIL")))

(defn ^:export run []
  (test/run-tests 'clara.test-rules
                  'clara.test-common
                  'clara.test-salience
                  'clara.test-testing-utils
                  'clara.test-complex-negation
                  'clara.test-accumulators))
