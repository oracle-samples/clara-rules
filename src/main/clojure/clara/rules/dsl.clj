(ns clara.rules.dsl
  "Implementation of the defrule-style DSL for Clara. Most users should simply use the clara.rules namespace."
  (:require [clojure.walk :as walk]
            [clara.rules.compiler :as com]
            [clara.rules.platform :as platform])
  (:refer-clojure :exclude [qualified-keyword?]))

;; Let operators be symbols or keywords.
(def ops #{'and 'or 'not 'exists :and :or :not :exists})

(defn- separator?
  "True iff `x` is a rule body separator symbol."
  [x] (and (symbol? x) (= "=>" (name x))))

(defn split-lhs-rhs
  "Given a rule with the =>, splits the left- and right-hand sides."
  [rule-body]
  (let [[pre [sep & post]] (split-with (complement separator?) rule-body)
        [lhs rhs] (if sep
                    [pre post]
                    ['() pre])]
    {:lhs lhs
     :rhs (when-not (empty? rhs)
            (conj rhs 'do))}))

(defn- throw-dsl-ex
  "Throws an exception indicating a failure parsing a form."
  [message info expr-meta]
  (if expr-meta
    (let [{:keys [line column file]} expr-meta]
      (throw (ex-info
              (str message
                   (when line
                     (str " line: " line))
                   (when column
                     (str " column: " column))
                   (when file
                     (str " file: " file)))

              (into info expr-meta))))
    (throw (ex-info message info))))

(defn- construct-condition
  "Creates a condition with the given optional result binding when parsing a rule."
  [condition result-binding expr-meta]
  (let [type (if (symbol? (first condition))
               (if-let [resolved (resolve (first condition))]

                 ;; If the type resolves to a var, grab its contents for the match.
                 (if (var? resolved)
                   (deref resolved)
                   resolved)

                 (first condition)) ; For ClojureScript compatibility, we keep the symbol if we can't resolve it.
               (first condition))
        ;; Args is an optional vector of arguments following the type.
        args (if (vector? (second condition)) (second condition) nil)
        constraints (vec (if args (drop 2 condition) (rest condition)))]

    (when (and (vector? type)
               (some seq? type))

      (throw-dsl-ex (str "Type " type " is a vector and appears to contain expressions. "
                         "Is there an extraneous set of brackets in the condition?")
                    {}
                    expr-meta))

    (when (> (count args) 1)
      (throw-dsl-ex "Only one argument can be passed to a condition."
                    {}
                    expr-meta))

    ;; Check if a malformed rule has a nested operator where we expect a type.
    (when (and (sequential? type)
               (seq type)
               (ops (first type)))
      (throw-dsl-ex (str "Attempting to bind into " result-binding
                         " nested expression: " (pr-str condition)
                         " Nested expressions cannot be bound into higher-level results")
                    {}
                    expr-meta))

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
  [condition expr-meta]
  ;; Grab the binding of the operation result, if present.
  (let [result-binding (if (= '<- (second condition)) (keyword (first condition)) nil)
        condition (if result-binding (drop 2 condition) condition)]

    (when (and (not= nil result-binding)
               (not= \? (first (name result-binding))))
      (throw-dsl-ex (str "Invalid binding for condition: " result-binding)
                    {}
                    expr-meta))

    ;; If it's an s-expression, simply let it expand itself, and assoc the binding with the result.
    (if (#{'from :from} (second condition)) ; If this is an accumulator....
      (let [parsed-accum {:accumulator (first condition)
                          :from (construct-condition (nth condition 2) nil expr-meta)}]
        ;; A result binding is optional for an accumulator.
        (if result-binding
          (assoc parsed-accum :result-binding result-binding)
          parsed-accum))
      ;; Not an accumulator, so simply create the condition.
      (construct-condition condition result-binding expr-meta))))

(defn- parse-expression
  "Convert each expression into a condition structure."
  [expression expr-meta]
  (cond

    (contains? ops (first expression))
    (into [(keyword (name (first expression)))] ; Ensure expression operator is a keyword.
          (for [nested-expr (rest expression)]
            (parse-expression nested-expr expr-meta)))

    (contains? #{'test :test} (first expression))
    (if (= 1 (count expression))
      (throw-dsl-ex (str "Empty :test conditions are not allowed.")
                    {}
                    expr-meta)
      {:constraints (vec (rest expression))})

    :else
    (parse-condition-or-accum expression expr-meta)))

(defn- maybe-qualify
  "Attempt to qualify the given symbol, returning the symbol itself
   if we can't qualify it for any reason."
  [env sym]
  (let [env (set env)]
    (if (and (symbol? sym)
             (not= '.. sym) ; Skip the special .. host interop macro.
             (.endsWith (name sym) "."))

      ;; The . suffix indicates a type constructor, so we qualify the type instead, then
      ;; re-add the . to the resolved type.
      (-> (subs (name sym)
                0
                (dec (count (name sym)))) ; Get the name without the trailing dot.
          (symbol) ; Convert it back to a symbol.
          ^Class (resolve) ; Resolve into the type class.
          (.getName) ; Get the qualified name.
          (str ".") ; Re-add the dot for the constructor, which is now qualified
          (symbol)) ; Convert back into a symbol used at compile time.

      ;; Qualify a normal clojure symbol.
      (if (and (symbol? sym) ; Only qualify symbols...
               (not (env sym))) ; not in env (env contains locals).) ; that we can resolve

        (cond
         ;; If it's a Java class, simply qualify it.
          (instance? Class (resolve sym))
          (symbol (.getName ^Class (resolve sym)))

         ;; Don't qualify clojure.core for portability, since CLJS uses a different namespace.
          (and (resolve sym)
               (not (= "clojure.core"
                       (str (ns-name (:ns (meta (resolve sym)))))))
               (name sym))
          (symbol (str (ns-name (:ns (meta (resolve sym))))) (name sym))

         ;; See if it's a static method call and qualify it.
          (and (namespace sym)
               (not (resolve sym))
               (instance? Class (resolve (symbol (namespace sym))))) ; The namespace portion is the class name we try to resolve.
          (symbol (.getName ^Class (resolve (symbol (namespace sym))))
                  (name sym))

         ;; Finally, just return the symbol unchanged if it doesn't match anything above,
         ;; assuming it's a local parameter or variable.n
          :else
          sym)
        sym))))

(defn- qualify-meta
  "Qualify metadata associated with the symbol."
  [env sym]
  (if (:tag (meta sym))
    (vary-meta sym update-in [:tag] (fn [tag] (-> ^Class (resolve tag)
                                                  (.getName)
                                                  (symbol))))
    sym))

(defn- resolve-vars
  "Resolve vars used in expression. TODO: this should be narrowed to resolve only
   those that aren't in the environment, condition, or right-hand side."
  [form env]
  (walk/postwalk
   (fn [sym]
     (->> sym
          (maybe-qualify env)
          (qualify-meta env)))
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
  "Creates a rule from the DSL syntax using the given environment map.  *ns*
   should be bound to the namespace the rule is meant to be defined in."
  ([lhs rhs properties env]
   (parse-rule* lhs rhs properties env {}))
  ([lhs rhs properties env rule-meta]
   (let [conditions (into [] (for [expr lhs]
                               (parse-expression expr rule-meta)))

         rule {:ns-name (list 'quote (ns-name *ns*))
               :lhs     (list 'quote
                              (mapv #(resolve-vars % (destructure-syms %))
                                    conditions))
               :rhs     (list 'quote
                              (vary-meta rhs
                                         assoc :file *file*))}

         symbols (set (filter symbol? (com/flatten-expression (concat lhs rhs))))
         matching-env (into {} (for [sym (keys env)
                                     :when (symbols sym)]
                                 [(keyword (name sym)) sym]))]

     (cond-> rule

       ;; Add properties, if given.
       (seq properties) (assoc :props properties)

       ;; Add the environment, if given.
       (seq env) (assoc :env matching-env)))))

(defn parse-rule-action*
  "Creates a rule action from the DSL syntax using the given environment map.  *ns*
   should be bound to the namespace the rule is meant to be defined in."
  ([lhs rhs properties env]
   (parse-rule-action* lhs rhs properties env {}))
  ([lhs rhs properties env rule-meta]
   (let [conditions (into [] (for [expr lhs]
                               (parse-expression expr rule-meta)))

         rule {:ns-name (ns-name *ns*)
               :lhs     (mapv #(resolve-vars % (destructure-syms %))
                              conditions)
               :rhs     (vary-meta rhs assoc :file *file*)}

         symbols (set (filter symbol? (com/flatten-expression (concat lhs rhs))))
         matching-env (into {} (for [sym (keys env)
                                     :when (symbols sym)]
                                 [(keyword (name sym)) sym]))]

     (cond-> rule

       ;; Add properties, if given.
       (seq properties) (assoc :props properties)

       ;; Add the environment, if given.
       (seq env) (assoc :env matching-env)))))

(defn parse-query*
  "Creates a query from the DSL syntax using the given environment map."
  ([params lhs env]
   (parse-query* params lhs env {}))
  ([params lhs env query-meta]
   (let [conditions (into [] (for [expr lhs]
                               (parse-expression expr query-meta)))

         query {:lhs (list 'quote (mapv #(resolve-vars % (destructure-syms %))
                                        conditions))
                :params (set (map platform/query-param params))}

         symbols (set (filter symbol? (com/flatten-expression lhs)))
         matching-env (into {}
                            (for [sym (keys env)
                                  :when (symbols sym)]
                              [(keyword (name sym)) sym]))]

     (cond-> query
       (seq env) (assoc :env matching-env)))))

(defmacro parse-rule
  "Macro used to dynamically create a new rule using the DSL syntax."
  ([lhs rhs]
   (parse-rule* lhs rhs nil &env))
  ([lhs rhs properties]
   (parse-rule* lhs rhs properties &env)))

;;; added to clojure.core in 1.9
(defn- qualified-keyword?
  "Return true if x is a keyword with a namespace"
  [x] (and (keyword? x) (namespace x) true))

(defn- production-name
  [prod-name]
  (cond
    (qualified-keyword? prod-name) prod-name
    :else (str (name (ns-name *ns*)) "/" (name prod-name))))

(defn build-rule
  "Function used to parse and build a rule using the DSL syntax."
  ([name body] (build-rule name body {}))
  ([name body form-meta]
   (let [doc (if (string? (first body)) (first body) nil)
         body (if doc (rest body) body)
         properties (if (map? (first body)) (first body) nil)
         definition (if properties (rest body) body)
         {:keys [lhs rhs]} (split-lhs-rhs definition)]
     (cond-> (parse-rule* lhs rhs properties {} form-meta)

       name (assoc :name (production-name name))
       doc (assoc :doc doc)))))

(defn build-rule-action
  "Function used to parse and build a rule action using the DSL syntax."
  ([name body] (build-rule-action name body {}))
  ([name body form-meta]
   (let [doc (if (string? (first body)) (first body) nil)
         body (if doc (rest body) body)
         properties (if (map? (first body)) (first body) nil)
         definition (if properties (rest body) body)
         {:keys [lhs rhs]} (split-lhs-rhs definition)]
     (cond-> (parse-rule-action* lhs rhs properties {} form-meta)

       name (assoc :name (production-name name))
       doc (assoc :doc doc)))))

(defmacro parse-query
  "Macro used to dynamically create a new rule using the DSL syntax."
  [params lhs]
  (parse-query* params lhs &env))

(defn build-query
  "Function used to parse and build a query using the DSL syntax."
  ([name body] (build-query name body {}))
  ([name body form-meta]
   (let [doc (if (string? (first body)) (first body) nil)
         binding (if doc (second body) (first body))
         definition (if doc (drop 2 body) (rest body))]
     (cond-> (parse-query* binding definition {} form-meta)
       name (assoc :name (production-name name))
       doc (assoc :doc doc)))))
