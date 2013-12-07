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
            [clojure.string :as string]
            [clara.rules.schema :as schema]
            [schema.core :as sc]))

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

(defn variables-as-keywords
  "Returns symbols in the given s-expression that start with '?' as keywords"
  [expression]
  (into #{} (for [item (flatten expression) 
                  :when (and (symbol? item) 
                             (= \? (first (name item))))] 
              (keyword  item))))

(defn compile-condition 
  "Returns a function definition that can be used in alpha nodes to test the condition."
  [type destructured-fact constraints result-binding env]
  (let [;; Get a map of fieldnames to access function symbols.
        accessors (get-fields type)

        binding-keys (variables-as-keywords constraints)
        ;; The assignments should use the argument destructuring if provided, or default to accessors otherwise.
        assignments (if destructured-fact
                      ;; Simply destructure the fact if arguments are provided.
                      [destructured-fact '?__fact__]
                      ;; No argument provided, so use our default destructuring logic.
                      (concat '(this ?__fact__)
                              (mapcat (fn [[name accessor]] 
                                        [name (list accessor '?__fact__)]) 
                                      accessors)))

        ;; The destructured environment, if any
        destructured-env (if (> (count env) 0)
                           {:keys (mapv #(symbol (name %)) (keys env))}
                           '?__env__)

        ;; Initial bindings used in the return of the compiled condition expresion.
        initial-bindings (if result-binding {result-binding 'this}  {})]

    `(fn [~(if (symbol? type) 
             (with-meta 
               '?__fact__ 
               {:tag (symbol (.getName type))})  ; Add type hint to avoid runtime refection.
             '?__fact__)
          ~destructured-env] ;; TODO: add destructured environment parameter...
       (let [~@assignments
             ~'?__bindings__ (atom ~initial-bindings)]
         (do ~@(compile-constraints constraints (set binding-keys)))))))

;; FIXME: add env...
(defn compile-test [tests]
  (let [binding-keys (variables-as-keywords tests)
        assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)]

    `(fn [~'?__token__] 
      (let [~@assignments]
        (and ~@tests)))))

(defn compile-action
  "Compile the right-hand-side action of a rule, returning a function to execute it."
  [binding-keys rhs env]
  (let [assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)
        
        ;; The destructured environment, if any.
        destructured-env (if (> (count env) 0)
                           {:keys (mapv #(symbol (name %)) (keys env))}
                           '?__env__)]
    `(fn [~'?__token__  ~destructured-env] 
       (let [~@assignments]
         ~rhs))))

(defn compile-accum
  [accum env]
  (let [destructured-env 
        (if (> (count env) 0)
          {:keys (mapv #(symbol (name %)) (keys env))}
          '?__env__)]
    `(fn [~destructured-env] 
       ~accum)))