(ns clara.rules.dsl
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [clojure.set :as s]
            [clara.rules.engine :as eng]
            [clojure.string :as string]
            [clara.rules.schema :as schema]
            [schema.core :as sc]
            [clojure.walk :as walk]))


;; Let operators be symbols or keywords.
(def ops #{'and 'or 'not :and :or :not})

(defn separator?
  "True iff `x` is a rule body separator symbol."
  [x] (and (symbol? x) (= "=>" (name x))))

(defn parse-rule-body [[head & more]]
  (cond
   ;; Detect the separator for the right-hand side.
   (separator? head) {:lhs (list) :rhs (first more)}

   ;; Handle a normal left-hand side element.
   (sequential? head) (update-in 
                       (parse-rule-body more)
                       [:lhs] conj head)

   ;; Handle the <- style assignment
   (symbol? head) (update-in 
                   (parse-rule-body (drop 2 more))
                   [:lhs] conj (conj head (take 2 more)))))

(defn parse-query-body [[head & more]]
  (cond
   (nil? head) (list)

   ;; Handle a normal left-hand side element.
   (sequential? head) (conj (parse-query-body more) head)

   ;; Handle the <- style assignment
   (symbol? head) (conj (parse-query-body (drop 2 more)) (conj head (take 2 more)))))



(defn- construct-condition [condition result-binding]
  (let [type (if (symbol? (first condition)) 
               (if-let [resolved (resolve (first condition))] 
                 resolved
                 (first condition)) ; For ClojureScript compatibility, we keep the symbol if we can't resolve it.
               (first condition))
        ;; Args is an optional vector of arguments following the type.
        args (if (vector? (second condition)) (second condition) nil)
        constraints (vec (if args (drop 2 condition) (rest condition)))]

    (when (> (count args) 1)
      (throw (IllegalArgumentException. "Only one argument can be passed to a condition.")))

    (cond-> {:type type
             :constraints (list 'quote constraints)}
            args (assoc :args (list 'quote args))
            result-binding (assoc :fact-binding result-binding))))

(defn parse-condition-or-accum [condition]
  ;; Grab the binding of the operation result, if present.
  (let [result-binding (if (= '<- (second condition)) (keyword (first condition)) nil)
        condition (if result-binding (drop 2 condition) condition)]

    (when (and (not= nil result-binding)
               (not= \? (first (name result-binding))))
      (throw (IllegalArgumentException. (str "Invalid binding for condition: " result-binding))))

    ;; If it's an s-expression, simply let it expand itself, and assoc the binding with the result.
    (if (#{'from :from} (second condition)) ; If this is an accumulator....
      {:result-binding result-binding
       :accumulator (list 'quote (first condition))
       :from (construct-condition (nth condition 2) nil)}
      
      ;; Not an accumulator, so simply create the condition.
      (construct-condition condition result-binding))))

(defn parse-expression
  "Convert each expression into a condition structure."
  [expression]
  (cond 

   (contains? ops (first expression))
   (into [(keyword (name (first expression)))] ; Ensure expression operator is a keyword.
         (map parse-expression (rest expression)))

   (contains? #{'test :test} (first expression))
   {:constraints (list 'quote (vec (rest expression)))}

   :default
   (parse-condition-or-accum expression)))


(defn resolve-vars
  "Resolve vars used in expression. TODO: this should be narrowed to resolve only
   those that aren't in the environment, condition, or right-hand side."
  [form]
  (walk/prewalk
   #(if (and (symbol? %) (resolve %)  
             (not (= "clojure.core" 
                     (str (ns-name (:ns (meta (resolve %))))))) (name %))
      (symbol (str (ns-name (:ns (meta (resolve %))))) (name %))
      %)
   form))

(defn parse-rule
  [lhs rhs properties env]
  (let [rule (resolve-vars 
              {:lhs (mapv parse-expression lhs)
               :rhs (list 'quote rhs)})

        symbols (set (filter symbol? (flatten (concat lhs rhs))))
        matching-env (into {} (for [sym (keys env)
                                    :when (symbols sym)
                                    ]
                                [(keyword (name sym)) sym]))]

    (cond-> rule
            
            ;; Add properties, if given.
            (not (empty? properties)) (assoc :props properties)
            
            ;; Add the environment, if given.
            (not (empty? env)) (assoc :env matching-env))))

(defn parse-query
  [params lhs env] 
  (let [query (resolve-vars 
               {:lhs (mapv parse-expression lhs)
                :params (set params)})

        symbols (set (filter symbol? (flatten lhs)))
        matching-env (into {}
                           (for [sym (keys env)
                                 :when (symbols sym)]
                             [(keyword (name sym)) sym]))]
    (cond-> query

     (not (empty? env)) (assoc :env matching-env))))


