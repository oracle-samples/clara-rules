(ns clara.macros
  "Forward-chaining rules for Clojure. The primary API is in this namespace"
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem]
            [clara.rules.compiler :as com]
            [cljs.analyzer :as ana])
  (import [clara.rules.engine LocalTransport LocalSession]))

(defmacro mk-query
  "Creates a new query based on a sequence of a conditions. 
   This is only used when creating queries dynamically; most users should use defquery instead."
  [params lhs]
  ;; TODO: validate params exist as keyworks in the query.
  `(eng/->Query
    ~params
    ~(com/parse-lhs lhs)
    ~(com/variables-as-keywords lhs)))

(defmacro mk-rule
  "Creates a new rule based on a sequence of a conditions and a righthand side. 
   This is only used when creating new rules directly; most users should use defrule instead."
  ([lhs rhs]
   `(mk-rule ~lhs ~rhs {}))
  ([lhs rhs properties]
     `(eng/->Production 
       ~(com/parse-lhs lhs)
       ~(com/compile-action (com/variables-as-keywords lhs) rhs)
       ~properties)))

(defmacro mk-session
   "Creates a new session using the given rule sources. Thew resulting session
   is immutable, and can be used with insert, retract, fire-rules, and query functions.

   If no sources are provided, it will attempt to load rules from the caller's namespace.

   The caller may also specify keyword-style options at the end of the parameters. Currently two
   options are supported:

   * :fact-type-fn, which must have a value of a function used to determine the logical type of a given 
     cache. Defaults to Clojures type function.
   * :cache, indicating whether the session creation can be cached, effectively memoizing mk-session. 
     Defaults to true. Callers may wish to set this to false when needing to dynamically reload rules."
  [& args]
  (if (and (seq args) (not (keyword? (first args))))
    `(apply clara.rules.engine/mk-session ~(vec args)) ; At least one namespace given, so use it.
    `(apply clara.rules.engine/mk-session (concat ['~ana/*cljs-ns*] ~(vec args)))))

(defmacro defrule
  "Defines a rule and stores it in the given var. For instance, a simple rule would look like this:

(defrule hvac-approval
  \"HVAC repairs need the appropriate paperwork, so insert a validation error if approval is not present.\"
  [WorkOrder (= type :hvac)]
  [:not [ApprovalForm (= formname \"27B-6\")]]
  =>
  (insert! (->ValidationError 
            :approval 
            \"HVAC repairs must include a 27B-6 form.\")))
  
  See the guide at https://github.com/rbrush/clara-rules/wiki/Guide for details."

  [name & body]
  (let [doc (if (string? (first body)) (first body) nil)
        body (if doc (rest body) body)
        properties (if (map? (first body)) (first body) nil)
        definition (if properties (rest body) body)
        {:keys [lhs rhs]} (com/parse-rule-body definition)]
    `(let [rule# (mk-rule ~lhs ~rhs ~properties)]
       (clara.rules/register-rule! '~ana/*cljs-ns* rule#)
       (def ~(vary-meta name assoc :rule true :doc doc) rule#))))

(defmacro defquery 
  "Defines a query and stored it in the given var. For instance, a simple query that accepts no
   parameters would look like this:
    
(defquery check-job
  \"Checks the job for validation errors.\"
  []
  [?issue <- ValidationError])

   See the guide at https://github.com/rbrush/clara-rules/wiki/Guide for details."

  [name & body]
  (let [doc (if (string? (first body)) (first body) nil)
        binding (if doc (second body) (first body))
        definition (if doc (drop 2 body) (rest body) )]
    `(let [query# (mk-query ~binding ~(com/parse-query-body definition))]
       (clara.rules/register-rule! '~ana/*cljs-ns* query#)  
       (def ~(vary-meta name assoc :query true :doc doc) query#))))

