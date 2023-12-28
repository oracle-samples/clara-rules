(ns user
  (:require [criterium.core :refer [report-result
                                    quick-benchmark] :as crit]
            [ham-fisted.api :as hf]
            [ham-fisted.mut-map :as hm]))

(comment
  (add-tap #'println)
  (remove-tap #'println)
  (tap> "foobar"))
