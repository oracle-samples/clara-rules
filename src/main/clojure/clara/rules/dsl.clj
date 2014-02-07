(ns clara.rules.dsl
  "Implementation of the defrule-style DSL for Clara. Most users should simply use the clara.rules namespace."
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [clojure.set :as s]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [clara.rules.engine :as eng]
            [clara.rules.compiler :as com]
            [clara.rules.schema :as schema]
            [cljs.analyzer :as ana] ; TODO: make CLJS dep optional?
            [schema.core :as sc]))


;; Let operators be symbols or keywords.
(def ops #{'and 'or 'not :and :or :not})

(defn- separator?
  "True iff `x` is a rule body separator symbol."
  [x] (and (symbol? x) (= "=>" (name x))))

(defn split-lhs-rhs 
 "Given a rule with the =>, splits the left- and right-hand sides."
 [rule-body]
 (let [[lhs [sep rhs]] (split-with #(not (separator? %))  rule-body)]
   
   {:lhs lhs
    :rhs rhs}))

(defn- construct-condition
  "Creates a condition with the given optional result binding when parsing a rule."
  [condition result-binding]
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
             :constraints constraints}
            args (assoc :args args)
            result-binding (assoc :fact-binding result-binding))))

(defn- parse-condition-or-accum 
  "Parse an expression that could be a condition or an accumulator."
  [condition]
  ;; Grab the binding of the operation result, if present.
  (let [result-binding (if (= '<- (second condition)) (keyword (first condition)) nil)
        condition (if result-binding (drop 2 condition) condition)]

    (when (and (not= nil result-binding)
               (not= \? (first (name result-binding))))
      (throw (IllegalArgumentException. (str "Invalid binding for condition: " result-binding))))

    ;; If it's an s-expression, simply let it expand itself, and assoc the binding with the result.
    (if (#{'from :from} (second condition)) ; If this is an accumulator....
      {:result-binding result-binding
       :accumulator  (first condition)
       :from (construct-condition (nth condition 2) nil)}
      
      ;; Not an accumulator, so simply create the condition.
      (construct-condition condition result-binding))))

(defn- parse-expression
  "Convert each expression into a condition structure."
  [expression]
  (cond 

   (contains? ops (first expression))
   (into [(keyword (name (first expression)))] ; Ensure expression operator is a keyword.
         (map parse-expression (rest expression)))

   (contains? #{'test :test} (first expression))
   {:constraints (vec (rest expression))}

   :default
   (parse-condition-or-accum expression)))

(defn- maybe-qualify
  "Attempt to qualify the given symbol, returning the symbol itself
   if we can't qualify it for any reason."
  [sym]
  (if (com/compiling-cljs?)

    ;; Qualify the symbol using the CLJS analyzer.
    (if-let [resolved (and (symbol? sym) 
                           (com/resolve-cljs-sym (com/cljs-ns) sym))]
      resolved
      sym)

    ;; Qualify the Clojure symbol.
    (if (and (symbol? sym) (resolve sym)  
             (not (= "clojure.core" 
                     (str (ns-name (:ns (meta (resolve sym))))))) (name sym))
      (symbol (str (ns-name (:ns (meta (resolve sym))))) (name sym))
      sym)))

(defn- resolve-vars
  "Resolve vars used in expression. TODO: this should be narrowed to resolve only
   those that aren't in the environment, condition, or right-hand side."
  [form]
  (walk/postwalk
   maybe-qualify
   form))

(defn parse-rule*
  "Creates a rule from the DSL syntax using the given environment map."
  [lhs rhs properties env]
  (let [rule (resolve-vars 
              {:lhs (list 'quote (mapv parse-expression lhs))
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

(defn parse-query*
  "Creates a query from the DSL syntax using the given environment map."
  [params lhs env] 
  (let [query (resolve-vars 
               {:lhs (list 'quote (mapv parse-expression lhs))
                :params (set params)})

        symbols (set (filter symbol? (flatten lhs)))
        matching-env (into {}
                           (for [sym (keys env)
                                 :when (symbols sym)]
                             [(keyword (name sym)) sym]))]

    (cond-> query
            (not (empty? env)) (assoc :env matching-env))))

(defmacro parse-rule
  "Macro used to dynamically create a new rule using the DSL syntax."
  ([lhs rhs]
     (parse-rule* lhs rhs nil &env))
  ([lhs rhs properties]
     (parse-rule* lhs rhs properties &env)))

(defmacro parse-query
  "Macro used to dynamically create a new rule using the DSL syntax."
  [params lhs]
  (parse-query* params lhs &env))
