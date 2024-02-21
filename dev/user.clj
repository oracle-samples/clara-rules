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

(def session-cache
  (cache/lru-cache-factory {}))

;; Cache of compiled expressions
(def compiler-cache
  (cache/soft-cache-factory {}))

(defmacro mk-rules
  [n]
  (let [facts (for [n (range n)]
                {:fact-type {:t (symbol (format "FactType%s" n))
                             :c (symbol (format "%s.FactType%s" (ns-name *ns*) n))}
                 :fact-record {:t (symbol (format "FactRecord%s" n))
                               :c (symbol (format "%s.FactRecord%s" (ns-name *ns*) n))}})
        type-declarations (for [{{:keys [t]} :fact-type} facts]
                            `(deftype ~t []))
        record-declarations (for [{{:keys [t]} :fact-record} facts]
                              `(defrecord ~t []))
        fact-rules (for [{:keys [fact-type
                                 fact-record]} facts]
                     `(hash-map
                       :ns-name (ns-name *ns*)
                       :lhs [{:type ~(:c fact-type)
                              :constraints []}
                             {:type ~(:c fact-record)
                              :constraints []}]
                       :rhs '(println (str "class:" ~n ~fact-type ~fact-record))))]
    `(do
       ~@type-declarations
       ~@record-declarations
       (vector
        ~@fact-rules))))

(comment
  (def rules
    (mk-rules 5000))
  (count (.cache ^clojure.core.cache.SoftCache @compiler-cache))

  (time
   (mk-session (conj rules {:ns-name (ns-name *ns*)
                            :lhs [{:type :foobar12
                                   :constraints []}]
                            :rhs `(println ~(str :foobar))})
               :cache false
               :compiler-cache compiler-cache)))
