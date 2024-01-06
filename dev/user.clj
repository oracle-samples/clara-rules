(ns user
  (:require [criterium.core :refer [report-result
                                    quick-benchmark] :as crit]
            [clara.rules.platform :refer [compute-for]]
            [clojure.core.async :refer [go timeout <!]]
            [ham-fisted.api :as hf]
            [ham-fisted.mut-map :as hm])
  (:import [java.util.concurrent CompletableFuture]))

(comment
  (add-tap #'println)
  (remove-tap #'println)
  (tap> "foobar"))
