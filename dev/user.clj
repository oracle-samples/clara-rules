(ns user
  (:require [criterium.core :refer [report-result
                                    quick-benchmark] :as crit]
            [ham-fisted.api :as hf]
            [ham-fisted.mut-map :as hm]
            [clara.rules.platform :as platform]))
