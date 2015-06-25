(ns clara.macros
  "Forward-chaining rules for Clojure. The primary API is in this namespace"
  (:require [clara.rules.engine.sessions :as session]
            [clara.rules.engine.nodes :as nodes]
            [clara.rules.engine.nodes.accumulators :as accs]
            [clara.rules.memory :as mem]
            [clara.rules.compiler.codegen :as codegen] 
            [clara.rules.compiler.expressions :as expr] [clara.rules.compiler.helpers :as hlp]
            [clara.rules.compiler.trees :as trees]
            [clara.rules.dsl :as dsl]            
            [cljs.analyzer :as ana]
            [cljs.env :as env]
            [clojure.set :as s]))


;; Store production in cljs.env/*compiler* under ::productions seq?
(defn- add-production [name production]
  (swap! env/*compiler* assoc-in [::productions (hlp/cljs-ns) name] production))


(defmacro defrule 
  [name & body]
  (let [doc (if (string? (first body)) (first body) nil)
        body (if doc (rest body) body)
        properties (if (map? (first body)) (first body) nil)
        definition (if properties (rest body) body)
        {:keys [lhs rhs]} (dsl/split-lhs-rhs definition)

        production (cond-> (dsl/parse-rule* lhs rhs properties {})
                           name (assoc :name (str (clojure.core/name (hlp/cljs-ns)) "/" (clojure.core/name name)))
                           doc (assoc :doc doc))]
    (add-production name production)
    `(def ~name
       ~production)))

(defmacro defquery 
  [name & body]
  (let [doc (if (string? (first body)) (first body) nil)
        binding (if doc (second body) (first body))
        definition (if doc (drop 2 body) (rest body) )

        query (cond-> (dsl/parse-query* binding definition {})
                      name (assoc :name (str (clojure.core/name (hlp/cljs-ns)) "/" (clojure.core/name name)))
                      doc (assoc :doc doc))]
    (add-production name query)
    `(def ~name
       ~query)))

(defmacro defsession 
  "Creates a sesson given a list of sources and keyword-style options, which are typically ClojureScript namespaces.

  Each source is eval'ed at compile time, in Clojure (not ClojureScript.)

  If the eval result is a symbol, it is presumed to be a ClojureScript
  namespace, and all rules and queries defined in that namespace will
  be found and used.

  If the eval result is a collection, it is presumed to be a
  collection of productions. Note that although the collection must
  exist in the compiling Clojure runtime (since the eval happens at
  macro-expansion time), any expressions in the rule or query
  definitions will be executed in ClojureScript.

  Typical usage would be like this, with a session defined as a var:

(defsession my-session 'example.namespace)

That var contains an immutable session that then can be used as a starting point to create sessions with
caller-provided data. Since the session itself is immutable, it can be safely used from multiple threads
and will not be modified by callers. So a user might grab it, insert facts, and otherwise
use it as follows:

   (-> my-session
     (insert (->Temperature 23))
     (fire-rules))  
"
  [name & sources-and-options]
  (let [sources (take-while #(not (keyword? %)) sources-and-options)
        options (apply hash-map (drop-while #(not (keyword? %)) sources-and-options))
        ;; Eval to unquote ns symbols, and to eval exprs to look up
        ;; explicit rule sources
        sources (eval (vec sources))
        productions (codegen/get-productions sources :cljs @env/*compiler*)
        {:keys [beta-trees alpha-trees]} (comp/compile->ast productions)
        beta-trees (comp/generate-beta-tree-expr beta-trees #{})]
    `(let [rulebase# (clara.rules.compiler.codegen/build-network ~beta-trees alpha-trees productions)]
       (def ~name (clara.rules.engine/rulebase->session rulebase# options#)))))

