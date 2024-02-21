(ns user
  (:require [criterium.core :refer [report-result
                                    quick-benchmark] :as crit]
            [clara.rules.platform :refer [compute-for]]
            [clojure.core.async :refer [go timeout <!]]
            [clara.rules :refer [mk-session]]
            [clara.rules.compiler :as com]
            [clojure.core.cache.wrapped :as cache]
            [schema.core :as sc]
            [ham-fisted.api :as hf]
            [ham-fisted.mut-map :as hm])
  (:import [java.util.concurrent CompletableFuture]))

(comment
  (add-tap #'println)
  (remove-tap #'println)
  (tap> "foobar"))

(defonce session-cache
  (cache/lru-cache-factory {}))

;; Cache of compiled expressions
(defonce compiler-cache
  (cache/soft-cache-factory {}))

(defonce rules
  (vec
   (for [n (range 5000)
         :let [fact-type (keyword (str "fact" n))]]
     {:ns-name (ns-name *ns*)
      :lhs [{:type fact-type
             :constraints []}]
      :rhs `(println ~(str fact-type))})))

(comment
  (count (.cache ^clojure.core.cache.SoftCache @compiler-cache))

  (time
   (mk-session (conj rules {:ns-name (ns-name *ns*)
                            :lhs [{:type :foobar12
                                   :constraints []}]
                            :rhs `(println ~(str :foobar))})
               :cache false
               :compiler-cache compiler-cache)))
