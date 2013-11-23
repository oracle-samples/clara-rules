(ns clara.rules.compiler
  "The Clara rules compiler, translating raw data structures into compiled versions and functions. 
   Most users should use only the clara.rules namespace."
  (:use clara.rules.memory
        clojure.pprint
        clojure.java.io)
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [clojure.set :as s]
            [clara.rules.engine :as eng]
            [clojure.string :as string]))

(def ^:private reflector
  "For some reason (bug?) the default reflector doesn't use the
  Clojure dynamic class loader, which prevents reflecting on
  `defrecords`.  Work around by supplying our own which does."
  (clojure.reflect.JavaReflector. (clojure.lang.RT/baseLoader)))

;; This technique borrowed from Prismatic's schema library.
(defn compiling-cljs?
  "Return true if we are currently generating cljs code.  Useful because cljx does not
         provide a hook for conditional macro expansion."
  []
  (boolean
   (when-let [n (find-ns 'cljs.analyzer)]
     (when-let [v (ns-resolve n '*cljs-file*)]
       @v))))

(defn get-namespace-info 
  "Get metadata about the given namespace."
  [namespace]
  (when-let [n (and (compiling-cljs?) (find-ns 'cljs.env))]
    (when-let [v (ns-resolve n '*compiler*)]
      (get-in @@v [ :cljs.analyzer/namespaces namespace]))))

(defn cljs-ns
  "Returns the ClojureScript namespace being compiled during Clojurescript compilation."
  []
  (if (compiling-cljs?)
    (-> 'cljs.analyzer (find-ns) (ns-resolve '*cljs-ns*) deref)
    nil))

(defn resolve-cljs-sym 
  "Resolves a ClojureScript symbol in the given namespace."
  [ns-sym sym]
  (let [ns-info (get-namespace-info ns-sym)]
    (if (namespace sym)

      ;; Symbol qualified by a namespace, so look it up in the requires info.
      (if-let [source-ns (get-in ns-info [:requires (namespace sym)])]
        (symbol (name source-ns) (name sym))
        (throw (RuntimeException. (str "Unable to resolve symbol: " sym " in namespace " ns-sym))))
      
      ;; Symbol is unqualified, so check in the uses block.
      (if-let [source-ns (get-in ns-info [:uses sym])]
        (symbol (name source-ns) (name sym))

        ;; Symbol not found in eiher block, so assume it is local.
        (symbol (name ns-sym) (name sym))))))

(defn- get-cljs-accessors 
  "Returns accessors for ClojureScript. WARNING: this touches 
  ClojureScript implementation details that may change."
  [sym]
  (let [resolved (resolve-cljs-sym (cljs-ns) sym)
        constructor (symbol (str "->" (name resolved)))
        namespace-info (get-namespace-info (symbol (namespace resolved)))
        constructor-info (get-in namespace-info [:defs constructor])]
    
    (if constructor-info
      (into {}
            (for [{field :name} (first (:method-params constructor-info))]
              [field (keyword (name field))]))
      [])))


(defn- get-field-accessors
  "Returns a map of field name to a symbol representing the function used to access it."
  [cls]
  (into {}
        (for [member (:members (reflect/type-reflect cls :reflector reflector))
              :when  (and (:type member) 
                          (not (#{'__extmap '__meta} (:name member)))
                          (:public (:flags member))
                          (not (:static (:flags member))))]
          [(symbol (string/replace (:name member) #"_" "-")) ; Replace underscore with idiomatic dash.
           (symbol (str ".-" (:name member)))])))

(defn- get-bean-accessors
  "Returns a map of bean property name to a symbol representing the function used to access it."
  [cls]
  (into {}
        ;; Iterate through the bean properties, returning tuples and the corresponding methods.
        (for [property (seq (.. java.beans.Introspector 
                                (getBeanInfo cls) 
                                (getPropertyDescriptors)))]

          [(symbol (string/replace (.. property (getName)) #"_" "-")) ; Replace underscore with idiomatic dash.
           (symbol (str "." (.. property (getReadMethod) (getName))))])))

(defn get-fields
  "Returns a map of field name to a symbol representing the function used to access it."
  [type]
  (cond
   (and (compiling-cljs?) (symbol? type)) (get-cljs-accessors type)
   (isa? type clojure.lang.IRecord) (get-field-accessors type)
   (class? type) (get-bean-accessors type) ; Treat unrecognized classes as beans.
   :default []))  ; Other types have no accessors.


(defn- compile-constraints [exp-seq assigment-set]
  (if (empty? exp-seq)
    `((deref ~'?__bindings__))
    (let [ [[cmp a b :as exp] & rest] exp-seq
           compiled-rest (compile-constraints rest assigment-set)
           containEq? (and (symbol? cmp) (let [cmp-str (name cmp)] (or (= cmp-str "=") (= cmp-str "==")))) 
           a-in-assigment (and containEq? (and (symbol? a) (assigment-set (keyword a))))
           b-in-assigment (and containEq? (and (symbol? b) (assigment-set (keyword b))))]
      (cond
       a-in-assigment
       (if b-in-assigment
         `((let [a-exist# (contains? (deref ~'?__bindings__) ~(keyword a))
                 b-exist# (contains? (deref ~'?__bindings__) ~(keyword b))]
             (when (and (not a-exist#) (not b-exist#)) (throw (Throwable. "Binding undefine variables")))
             (when (not a-exist#) (swap! ~'?__bindings__ assoc ~(keyword a) ((deref ~'?__bindings__) ~(keyword b))))
             (when (not b-exist#) (swap! ~'?__bindings__ assoc ~(keyword b) ((deref ~'?__bindings__) ~(keyword a))))
             (if (or (not a-exist#) (not b-exist#) (= ((deref ~'?__bindings__) ~(keyword a)) ((deref ~'?__bindings__) ~(keyword b))))
               (do ~@compiled-rest)
               nil))) 
         (cons `(swap! ~'?__bindings__ assoc ~(keyword a) ~b) compiled-rest))
       b-in-assigment
       (cons `(swap! ~'?__bindings__ assoc ~(keyword b) ~a) compiled-rest)
       ;; not a unification
       :else
       (list (list 'if exp (cons 'do compiled-rest) nil))))))  

(defn- compile-condition 
  "Returns a function definition that can be used in alpha nodes to test the condition."
  [type destructured-fact constraints binding-keys result-binding]
  (let [;; Get a map of fieldnames to access function symbols.
        accessors (get-fields type)

        ;; The assignments should use the argument destructuring if provided, or default to accessors otherwise.
        assignments (if destructured-fact
                      ;; Simply destructure the fact if arguments are provided.
                      [destructured-fact '?__fact__]
                      ;; No argument provided, so use our default destructuring logic.
                      (concat '(this ?__fact__)
                              (mapcat (fn [[name accessor]] 
                                        [name (list accessor '?__fact__)]) 
                                      accessors)))

        ;; Initial bindings used in the return of the compiled condition expresion.
        initial-bindings (if result-binding {result-binding 'this}  {})]

    `(fn [~(if (symbol? type) 
             (with-meta 
               '?__fact__ 
               {:tag (symbol (.getName type))})  ; Add type hint to avoid runtime refection.
             '?__fact__)]

       (let [~@assignments
             ~'?__bindings__ (atom ~initial-bindings)]
         (do ~@(compile-constraints constraints (set binding-keys)))))))

(defn compile-action
  "Compile the right-hand-side action of a rule, returning a function to execute it."
  [binding-keys rhs]
  (let [assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)]
    `(fn [~'?__token__] 
       (let [~@assignments]
         ~rhs))))

(defn variables-as-keywords
  "Returns symbols in the given s-expression that start with '?' as keywords"
  [expression]
  (into #{} (for [item (flatten expression) 
                  :when (and (symbol? item) 
                             (= \? (first (name item))))] 
              (keyword  item))))

;; Record representing a compiled accumulator definition.
(defrecord AccumulatorDef [initial-value reduce-fn combine-fn convert-return-fn])

(defn- construct-condition [condition result-binding]
  (let [type (if (symbol? (first condition)) 
               (if-let [resolved (resolve (first condition))] 
                 resolved
                 (first condition)) ; For ClojureScript compatibility, we keep the symbol if we can't resolve it.
               (first condition))
        ;; Args is an optional vector of arguments following the type.
        args (if (vector? (second condition)) (second condition) nil)
        constraints (vec (if args (drop 2 condition) (rest condition)))
        binding-keys (variables-as-keywords constraints)
        text (pr-str condition)]

    (when (> (count args) 1)
      (throw (IllegalArgumentException. "Only one argument can be passed to a condition.")))
    
    `(eng/map->Condition {:type ~type 
                          :args '~(first args)
                          :constraints '~constraints 
                          :binding-keys ~binding-keys 
                          :activate-fn ~(compile-condition type (first args) constraints binding-keys result-binding)
                          :text ~text})))

(defn create-condition [condition]
  ;; Grab the binding of the operation result, if present.
  (let [result-binding (if (= '<- (second condition)) (keyword (first condition)) nil)
        condition (if result-binding (drop 2 condition) condition)]

    (when (and (not= nil result-binding)
               (not= \? (first (name result-binding))))
      (throw (IllegalArgumentException. (str "Invalid binding for condition: " result-binding))))

    ;; If it's an s-expression, simply let it expand itself, and assoc the binding with the result.
    (if (#{'from :from} (second condition)) ; If this is an accumulator....
      `(eng/map->Accumulator {:result-binding ~result-binding
                              :definition ~(first condition)
                              :input-condition ~(construct-condition (nth condition 2) nil)
                              :text ~(pr-str condition)})
      
      ;; Not an accumulator, so simply create the condition.
      (construct-condition condition result-binding))))

;; Let operators be symbols or keywords.
(def operators #{'and 'or 'not :and :or :not})

(defn mk-test [tests]
  (let [binding-keys (variables-as-keywords tests)
        assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)]
    `(eng/->Test 
      (fn [~'?__token__] 
        (let [~@assignments]
          (and ~@tests)))
      '~tests
      ~(pr-str tests))))

(defn- parse-expression [expression]
  (cond 
   (operators (first expression))
   {:type (keyword (first expression)) 
    :content (vec (map parse-expression (rest expression)))}

   (#{'test :test} (first expression))
   {:type :test 
    :content (mk-test (rest expression))}

   :default
   {:type :condition :content (create-condition expression)}))

(defn parse-lhs
  "Parse the left-hand side and returns an AST"
  [lhs] 
  (parse-expression 
   (if (operators (first lhs))
     lhs
     (cons 'and lhs)))) ; "and" is implied if a list of constraints are given without an operator.

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
