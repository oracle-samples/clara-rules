(ns user
  (:require [criterium.core :refer [report-result
                                    quick-benchmark] :as crit]
            [clara.rules.platform :refer [compute-for]]
            [clojure.core.async :refer [go timeout <!]]
            [clara.rules :refer [defrule mk-session clear-ns-productions!]]
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

(defmacro mk-types
  [n]
  (let [facts (for [n (range n)]
                {:fact-type {:t (symbol (format "FactType%s" n))
                             :c (symbol (format "%s.FactType%s" (ns-name *ns*) n))}
                 :fact-record {:t (symbol (format "FactRecord%s" n))
                               :c (symbol (format "%s.FactRecord%s" (ns-name *ns*) n))}})
        type-declarations (for [{{:keys [t]} :fact-type} facts]
                            `(deftype ~t []))
        record-declarations (for [{{:keys [t]} :fact-record} facts]
                              `(defrecord ~t []))]
    `(do
       ~@type-declarations
       ~@record-declarations)))

(defmacro mk-rules
  [n]
  (let [facts (for [n (range n)]
                {:decl-name (symbol (format "rule-%s" n))
                 :fact-type {:t (symbol (format "FactType%s" n))
                             :c (symbol (format "%s.FactType%s" (ns-name *ns*) n))}
                 :fact-record {:t (symbol (format "FactRecord%s" n))
                               :c (symbol (format "%s.FactRecord%s" (ns-name *ns*) n))}})
        fact-rules (for [{:keys [fact-type
                                 fact-record]} facts]
                     `(hash-map
                       :ns-name (ns-name *ns*)
                       :lhs [{:type ~(:c fact-type)
                              :constraints []}
                             {:type ~(:c fact-record)
                              :constraints []}]
                       :rhs '(println (str "class:" ~n ~fact-type ~fact-record))))
        decl-rules (for [{:keys [decl-name
                                 fact-type
                                 fact-record]} facts]
                     `(defrule ~decl-name
                        [~(:c fact-type)]
                        [~(:c fact-record)]
                        =>
                        (println (str "class:" ~n ~fact-type ~fact-record))))]
    `(do
       ~@decl-rules
       (vector
        ~@fact-rules))))

(comment
  (clear-ns-productions!)
  (mk-types 2500)
  (def rules
    (mk-rules 2500))
  (keys @session-cache)
  (when-let [v (first (.cache ^clojure.core.cache.SoftCache @compiler-cache))]
    (.getValue v))
  (count @session-cache)
  (count (.cache ^clojure.core.cache.SoftCache @compiler-cache))

  (time
   (mk-session 'user [(conj rules {:ns-name (ns-name *ns*)
                                   :lhs [{:type :foobar16
                                          :constraints []}]
                                   :rhs `(println ~(str :foobar))})]
               :cache session-cache
               :compiler-cache compiler-cache)))
