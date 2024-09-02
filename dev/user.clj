(ns user
  (:require [criterium.core :refer [report-result
                                    quick-benchmark
                                    with-progress-reporting] :as crit]
            [clara.rules :refer [defrule defquery defhierarchy
                                 insert! insert-all!
                                 insert insert-all
                                 fire-rules
                                 query
                                 mk-session
                                 clear-ns-vars!]]
            [clojure.core.cache.wrapped :as cache]))

(comment
  (clear-ns-vars!)
  (add-tap #'println)
  (remove-tap #'println)
  (tap> "foobar"))

(defhierarchy foobar
  :thing/foo :thing/that
  :thing/bar :thing/that)

(defrule return-a-thing
  [:thing/that [{:keys [value]}] (= value ?value)]
  =>
  (insert! {:type :thing/result
            :value ?value}))

(defquery query-a-thing
  []
  [?output <- :thing/result])

(defrule default-data
  (insert-all!
   [{:type :thing/foo
     :value 1}
    {:type :thing/bar
     :value 2}
    {:type :thing/bar
     :value 3}]))

(comment
  (time
   (-> (mk-session 'user :fact-type-fn :type)
       (fire-rules)
       (query query-a-thing))))

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
  (clear-ns-vars!)
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
                                   :lhs [{:type :foobar1
                                          :constraints []}]
                                   :rhs `(println ~(str :foobar))})]
               :cache session-cache
               :compiler-cache compiler-cache)))
