(ns clara.rules
  "Forward-chaining rules for Clojure. The primary API is in this namespace"
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem]
            [clara.rules.compiler :as com])
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

(defn add-productions
  "Returns a new rulebase identical to the given one, but with the additional
   rules or queries. This is only used when dynamically adding rules to a rulebase."
  [rulebase & productions]
  (-> (concat (:productions rulebase) (:queries rulebase) productions)
      (eng/shred-rules)
      (eng/compile-shredded-rules)))

;; Treate a symbol as a rule source, loading all items in its namespace.
(comment ;; FIXME -- use global registry!
  (extend-type clojure.lang.Symbol
    eng/IRuleSource
    (load-rules [sym]

      ;; Find the rules and queries in the namespace, shred them,
      ;; and compile them into a rule base.
      (->> (ns-interns sym)
           (vals) ; Get the references in the namespace.
           (filter #(or (:rule (meta %)) (:query (meta %)))) ; Filter down to rules and queries.
           (map deref) ; Get the rules from the symbols.
           (eng/shred-rules) ; Shred the rules.
           (eng/compile-shredded-rules))))) ; Compile into a knowledge base.

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
    `(def ~(vary-meta name assoc :rule true :doc doc)
       (mk-rule ~lhs ~rhs ~properties))))

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
    `(def ~(vary-meta name assoc :query true :doc doc) 
       (mk-query ~binding ~(com/parse-query-body definition)))))

