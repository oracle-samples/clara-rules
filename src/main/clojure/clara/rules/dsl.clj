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

    ;; Include the original metadata in the returned condition so line numbers
    ;; can be preserved when we compile it.
    (with-meta 
      (cond-> {:type type
               :constraints constraints}
              args (assoc :args args)
              result-binding (assoc :fact-binding result-binding))

      (if (seq constraints)
        (assoc (meta (first constraints))
          :file *file*)))))

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
      (let [parsed-accum {:accumulator (first condition)
                          :from (construct-condition (nth condition 2) nil)}]
        ;; A result binding is optional for an accumulator.
        (if result-binding
          (assoc parsed-accum :result-binding result-binding)
          parsed-accum))
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
  [env sym]
  (let [env (set env)]
    (if (com/compiling-cljs?)

      ;; Qualify the symbol using the CLJS analyzer.
      (if-let [resolved (and (symbol? sym)
                             (not (env sym))
                             (com/resolve-cljs-sym (com/cljs-ns) sym))]
        resolved
        sym)

      ;; Qualify the Clojure symbol.
      (if (and (symbol? sym) (not (env sym)) (resolve sym)
               (not (= "clojure.core"
                       (str (ns-name (:ns (meta (resolve sym))))))) (name sym))
        (symbol (str (ns-name (:ns (meta (resolve sym))))) (name sym))
        sym))))

(defn- resolve-vars
  "Resolve vars used in expression. TODO: this should be narrowed to resolve only
   those that aren't in the environment, condition, or right-hand side."
  [form env]
  (walk/postwalk
   #(maybe-qualify env %)
   form))

(defmacro local-syms []
  (mapv #(list 'quote %) (keys &env)))

(defn destructuring-sym? [sym]
  (or (re-matches #"vec__\d+" (name sym))
      (re-matches #"map__\d+" (name sym))))

(defn- destructure-syms
  [{:keys [args] :as condition}]
  (if args
    (remove destructuring-sym? (eval `(let [~args nil] (local-syms))))))

(defn parse-rule*
  "Creates a rule from the DSL syntax using the given environment map."
  [lhs rhs properties env]
  (let [conditions (mapv parse-expression lhs)
        rule {:lhs (list 'quote
                         (mapv #(resolve-vars % (destructure-syms %))
                               conditions))
              :rhs (with-meta
                     (resolve-vars (list 'quote rhs)
                                   (map :fact-binding conditions))
                     (assoc (meta rhs) :file *file*))}

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
  (let [conditions (mapv parse-expression lhs)
        query {:lhs (list 'quote (mapv #(resolve-vars % (destructure-syms %))
                                       conditions))
               :params (set params)}

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
