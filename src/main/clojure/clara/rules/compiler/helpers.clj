(ns clara.rules.compiler.helpers
  (:require
    [clojure.reflect :as reflect]
    [clojure.string :as string])
  (:import [java.beans PropertyDescriptor]))


(def ^:private reflector
  "For some reason (bug?) the default reflector doesn't use the
       Clojure dynamic class loader, which prevents reflecting on
       `defrecords`.  Work around by supplying our own which does."
  (clojure.reflect.JavaReflector. (clojure.lang.RT/makeClassLoader)))

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
        ;; Not in the requires block, so assume the qualified name is a refers and simply return the symbol.
        sym)

      ;; Symbol is unqualified, so check in the uses block.
      (if-let [source-ns (get-in ns-info [:uses sym])]
        (symbol (name source-ns) (name sym))

        ;; Symbol not found in eiher block, so attempt to retrieve it from
        ;; the current namespace.
        (if (get-in (get-namespace-info ns-sym) [:defs sym])
          (symbol (name ns-sym) (name sym))
          nil)))))

(defn get-cljs-accessors
  "Returns accessors for ClojureScript. WARNING: this touches
  ClojureScript implementation details that may change."
  [sym]
  (let [resolved (resolve-cljs-sym (cljs-ns) sym)
        constructor (symbol (str "->" (name resolved)))
        namespace-info (get-namespace-info (symbol (namespace resolved)))
        constructor-info (get-in namespace-info [:defs constructor])]

    (if constructor-info
      (into {}
            (for [field (first (:method-params constructor-info))]
              [field (keyword (name field))]))
      [])))


(defn get-field-accessors
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

(defn get-bean-accessors
  "Returns a map of bean property name to a symbol representing the function used to access it."
  [cls]
  (into {}
        ;; Iterate through the bean properties, returning tuples and the corresponding methods.
        (for [^PropertyDescriptor property (seq (.. java.beans.Introspector
                                                    (getBeanInfo cls)
                                                    (getPropertyDescriptors)))]

          [(symbol (string/replace (.. property (getName)) #"_" "-")) ; Replace underscore with idiomatic dash.
           (symbol (str "." (.. property (getReadMethod) (getName))))])))

(defn effective-type [type]
  (if (compiling-cljs?)
    type

    (if (symbol? type)
      (.loadClass (clojure.lang.RT/makeClassLoader) (name type))
      type)))

(defn get-fields
  "Returns a map of field name to a symbol representing the function used to access it."
  [type]
  (if (compiling-cljs?)

    ;; Get ClojureScript fields.
    (if (symbol? type)
      (get-cljs-accessors type)
      [])

    ;; Attempt to load the corresponding class for the type if it's a symbol.
    (let [type (effective-type type)]

      (cond
       (isa? type clojure.lang.IRecord) (get-field-accessors type)
       (class? type) (get-bean-accessors type) ; Treat unrecognized classes as beans.
       :default []))))

(defn add-meta
  "Helper function to add metadata."
  [fact-symbol fact-type]
  (if (class? fact-type)
    (vary-meta fact-symbol assoc :tag (symbol (.getName ^Class fact-type)))
    fact-symbol))
