(ns clara.rules
  "Forward-chaining rules for Clojure. The primary API is in this namespace"
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem])
  (import [clara.rules.engine LocalTransport LocalSession]))

(defn mk-rulebase 
  "Creates a rulebase with the given productions. This is only used when generating rulebases dynamically."
  [& productions]
  (if (seq productions)
    (eng/compile-shredded-rules (eng/shred-rules productions))
    (eng/->Rulebase {} [] [] [] [] {} {})))

(defn insert
  "Inserts one or more facts into a working session. It does not modify the given
   session, but returns a new session with the facts added."
  [session & facts] 
  (eng/insert session facts))

(defn retract
  "Retracts a fact from a working session. It does not modify the given session,
   but returns a new session with the facts retracted."
  [session & facts] 
  (eng/retract session facts))

(defn fire-rules 
  "Fires are rules in the given session. Once a rule is fired, it is labeled in a fired
   state and will not be re-fired unless facts affecting the rule are added or retracted.

   This function does not modify the given session to mark rules as fired. Instead, it returns
   a new session in which the rules are marked as fired."
  [session]
  (eng/fire-rules session))

(defn query 
  "Runs the given query with the optional given parameters against the session.
   The optional parameters should be in map form. For example, a query call might be:

   (query session get-by-last-name :last-name \"Jones\")
   "
  [session query & params]
  (eng/query session query (apply hash-map params)))

(defn- insert-facts! 
  "Perform the actual fact insertion, optionally making them unconditional."
  [facts unconditional] 
  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn]} eng/*current-session*
        {:keys [node token]} eng/*rule-context*]

    ;; Update the insertion count.
    (swap! insertions + (count facts))

    (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
            root alpha-roots]

      ;; Track this insertion in our transient memory so logical retractions will remove it.
      (when (not unconditional)
        (mem/add-insertions! transient-memory node token facts))

      (eng/alpha-activate root fact-group transient-memory transport))))

(defn insert! 
  "To be executed within a rule's right-hand side, this inserts a new fact or facts into working memory.

   Inserted facts are logical, in that if the support for the insertion is removed, the fact
   will automatically be retracted. For instance, if there is a rule that inserts a \"Cold\" fact
   if a \"Temperature\" fact is below a threshold, and the \"Temperature\" fact that triggered
   the rule is retracted, the \"Cold\" fact the rule inserted is also retracted. This is the underlying
   truth maintenance facillity.

   This truth maintenance is also transitive: if a rule depends on some criteria to fire, and a 
   criterion becomes invalid, it may retract facts that invalidate other rules, which in turn
   retract their conclusions. This way we can ensure that information inferred by rules is always
   in a consistent state."
  [& facts]
  (insert-facts! facts false))

(defn insert-unconditional! 
  "To be executed within a rule's right-hand side, this inserts a new fact or facts into working memory.

   This differs from insert! in that it is unconditional. The facts inserted will not be retracted
   even if the rule activation doing the insert becomes false.  Most users should prefer the simple insert!
   function as described above, but this function is available for use cases that don't wish to use
   Clara's truth maintenance."
  [& facts]
  (insert-facts! facts true))

(defn retract!
  "To be executed within a rule's right-hand side, this retracts a fact or facts from the working memory.

   Retracting facts from the right-hand side has slightly different semantics than insertion. As described
   in the insert! documentation, inserts are logical and will automatically be retracted if the rule
   that inserted them becomes false. This retract! function does not follow the inverse; retracted items
   are simply removed, and not re-added if the rule that retracted them becomes false.

   The reason for this is that retractions remove information from the knowledge base, and doing truth
   maintenance over retractions would require holding onto all retracted items, which would be an issue
   in some use cases. This retract! method is included to help with certain use cases, but unless you 
   have a specific need, it is better to simply do inserts on the rule's right-hand side, and let
   Clara's underlying truth maintenance retract inserted items if their support becomes false."
  [& facts]
  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn]} eng/*current-session*]

    ;; Update the count so the rule engine will know when we have normalized.
    (swap! insertions + (count facts))

    (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
            root alpha-roots]

      (eng/alpha-retract root fact-group transient-memory transport))))

(defmacro mk-query
  "Creates a new query based on a sequence of a conditions. 
   This is only used when creating queries dynamically; most users should use defquery instead."
  [params lhs]
  ;; TODO: validate params exist as keyworks in the query.
  `(eng/->Query
    ~params
    ~(eng/parse-lhs lhs)
    ~(eng/variables-as-keywords lhs)))

(defmacro mk-rule
  "Creates a new rule based on a sequence of a conditions and a righthand side. 
   This is only used when creating new rules directly; most users should use defrule instead."
  [lhs rhs]
  `(eng/->Production 
    ~(eng/parse-lhs lhs)
    ~(eng/compile-action (eng/variables-as-keywords lhs) rhs)))

(defn accumulate 
  "Creates a new accumulator based on the given properties:

   * An initial-value to be used with the reduced operations.
   * A reduce-fn that can be used with the Clojure Reducers library to reduce items.
   * A combine-fn that can be used with the Clojure Reducers library to combine reduced items.
   * An optional retract-fn that can remove a retracted fact from a previously reduced computation
   * An optional convert-return-fn that converts the reduced data into something useful to the caller.
     Simply uses identity by default.
    "
  [& {:keys [initial-value reduce-fn combine-fn retract-fn convert-return-fn] :as args}]
  (eng/map->Accumulator
   (merge
    {:combine-fn reduce-fn ; Default combine function is simply the reduce.
     :convert-return-fn identity ; Default conversion does nothing, so use identity.
     :retract-fn (fn [reduced retracted] reduced) ; Retractions do nothing by default.
     }
    args)))

(defn add-productions
  "Returns a new rulebase identical to the given one, but with the additional
   rules or queries. This is only used when dynamically adding rules to a rulebase."
  [rulebase & productions]
  (-> (concat (:productions rulebase) (:queries rulebase) productions)
      (eng/shred-rules)
      (eng/compile-shredded-rules)))

(defn mk-session* 
  "Creates a new session using the given rule source. Thew resulting session
   is immutable, and can be used with insert, retract, fire-rules, and query functions."
  ([source & more]
     ;; Merge all of the sources together and create a session.
     (let [rulebase (eng/load-rules source)
           transport (LocalTransport.)
           other-sources (take-while (complement keyword?) more)
           options (apply hash-map (drop-while (complement keyword?) more))

           ;; Merge other rule sessions into one.
           merged-rules (reduce           
                         (fn [rulebase other-source]
                           (eng/conj-rulebases rulebase (eng/load-rules other-source)))
                         (eng/load-rules source)
                         other-sources)

           ;; The fact-type uses Clojure's type function unless overridden.
           fact-type-fn (get options :fact-type-fn type)

           ;; Create a function that groups a sequence of facts by the collection
           ;; of alpha nodes they target.
           ;; We cache an alpha-map for facts of a given type to avoid computing
           ;; them for every fact entered.
           get-alphas-fn (eng/create-get-alphas-fn fact-type-fn merged-rules)]

       (LocalSession. merged-rules (eng/local-memory merged-rules transport) transport get-alphas-fn))))

(defmacro mk-session
   "Creates a new session using the given rule sources. Thew resulting session
   is immutable, and can be used with insert, retract, fire-rules, and query functions.

   If no sources are provided, it will attempt to load rules from the caller's namespace."
  [& args]
  (if (and (seq args) (not (keyword? (first args))))
    `(apply mk-session* ~(vec args)) ; At least one namespace given, so use it.
    `(apply mk-session* (concat [(ns-name *ns*)] ~(vec args))))) ; No namespace given, so use the current one.

(defn- separator?
  "True iff `x` is a rule body separator symbol."
  [x] (and (symbol? x) (= "=>" (name x))))

(defn- parse-rule-body [[head & more]]
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

(defn- parse-query-body [[head & more]]
  (cond
   (nil? head) (list)

   ;; Handle a normal left-hand side element.
   (sequential? head) (conj (parse-query-body more) head)

   ;; Handle the <- style assignment
   (symbol? head) (conj (parse-query-body (drop 2 more)) (conj head (take 2 more)))))

;; Treate a symbol as a rule source, loading all items in its namespace.
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
         (eng/compile-shredded-rules)))) ; Compile into a knowledge base.

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
        definition (if doc (rest body) body)
        {:keys [lhs rhs]} (parse-rule-body definition)]
    `(def ~(vary-meta name assoc :rule true :doc doc)
       (mk-rule ~lhs ~rhs))))

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
       (mk-query ~binding ~(parse-query-body definition)))))

