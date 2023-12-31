(ns user
  (:require [criterium.core :refer [report-result
                                    quick-benchmark] :as crit]
            [clara.rules.platform :refer [compute-for
                                          produce-for] :as platform]
            [clojure.core.async :refer [go timeout <!]]
            [ham-fisted.api :as hf]
            [ham-fisted.mut-map :as hm])
  (:import [java.util.concurrent CompletableFuture]))

(comment
  (report-result
   (quick-benchmark
    (binding [platform/*parallel-compute* true]
      (->> (produce-for
            [v (range 100)]
            (apply + v (range v)))
           (apply +)))
    {:verbose true}))

  (report-result
   (quick-benchmark
    (binding [platform/*parallel-compute* true]
      (->> (compute-for
            [v (range 100)]
            (apply + v (range v)))
           (apply +)))
    {:verbose true}))

  (report-result
   (quick-benchmark
    (binding [platform/*parallel-compute* true]
      (->> (for
            [v (range 100)
             (apply + v (range v))])
           (apply +)))
    {:verbose true}))

  (add-tap #'println)
  (remove-tap #'println)
  (tap> "foobar"))
