(ns clara.rules
  "Forward-chaining rules for Clojure. The primary API is in this namespace."
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem]
            [clara.rules.compiler :as com]
            [clara.rules.schema :as schema]
            [clara.rules.dsl :as dsl]
            [clara.rules.listener :as l]
            [schema.core :as sc])
  (import [clara.rules.engine LocalTransport LocalSession]))

(defn insert
  "Inserts one or more facts into a working session. It does not modify the given
   session, but returns a new session with the facts added."
  [session & facts]
  (eng/insert session facts))

(defn insert-all
  "Inserts a sequence of facts into a working session. It does not modify the given
   session, but returns a new session with the facts added."
  [session fact-seq]
  (eng/insert session fact-seq))

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

   The query itself may be either the var created by a defquery statement,
   or the actual name of the query.
   "
  [session query & params]
  (eng/query session query (apply hash-map params)))

(defn- insert-facts!
  "Perform the actual fact insertion, optionally making them unconditional."
  [facts unconditional]
  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} eng/*current-session*
        {:keys [node token]} eng/*rule-context*]

    ;; Update the insertion count.
    (swap! insertions + (count facts))

    ;; Track this insertion in our transient memory so logical retractions will remove it.
    (if unconditional
      (l/insert-facts! listener facts)
      (do
        (mem/add-insertions! transient-memory node token facts)
        (l/insert-facts-logical! listener node token facts)
        ))

    (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
            root alpha-roots]

      (eng/alpha-activate root fact-group transient-memory transport listener))))

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
  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} eng/*current-session*]

    ;; Update the count so the rule engine will know when we have normalized.
    (swap! insertions + (count facts))

    (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
            root alpha-roots]

      (eng/alpha-retract root fact-group transient-memory transport listener))))

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


;; Cache of sessions for fast reloading.
(def ^:private session-cache (atom {}))

(defmacro mk-session
   "Creates a new session using the given rule sources. Thew resulting session
   is immutable, and can be used with insert, retract, fire-rules, and query functions.

   If no sources are provided, it will attempt to load rules from the caller's namespace.

   The caller may also specify keyword-style options at the end of the parameters. Currently two
   options are supported:

   * :fact-type-fn, which must have a value of a function used to determine the logical type of a given
     cache. Defaults to Clojures type function.
   * :cache, indicating whether the session creation can be cached, effectively memoizing mk-session.
     Defaults to true. Callers may wish to set this to false when needing to dynamically reload rules.
   * :activation-group-fn, a function applied to Activation objects and returns the group they should be activated with.
     It defaults to checking the :salience property on the rule definition itself, or 0 if none exists.
   * :activation-group-sort-fn, a comparator function used to sort the values returned by the above :activation-group-fn.
     defaults to >, so rules with a higher salience are executed first.

   This is not supported in ClojureScript, since it requires eval to dynamically build a session. ClojureScript
   users must use pre-defined rulesessions using defsession."
  [& args]
  (if (and (seq args) (not (keyword? (first args))))
    `(com/mk-session ~(vec args)) ; At least one namespace given, so use it.
    `(com/mk-session (concat [(ns-name *ns*)] ~(vec args))))) ; No namespace given, so use the current one.

;; Treate a symbol as a rule source, loading all items in its namespace.
(extend-type clojure.lang.Symbol
  com/IRuleSource
  (load-rules [sym]

    ;; Find the rules and queries in the namespace, shred them,
    ;; and compile them into a rule base.
    (->> (ns-interns sym)
         (vals) ; Get the references in the namespace.
         (filter #(or (:rule (meta %)) (:query (meta %)))) ; Filter down to rules and queries.
         (map deref))))  ; Get the rules from the symbols.

(defmacro defsession
  "Creates a sesson given a list of sources and keyword-style options, which are typically Clojure namespaces.

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

  `(def ~name (com/mk-session ~(vec sources-and-options))))

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
        {:keys [lhs rhs]} (dsl/split-lhs-rhs definition)]
    (when-not rhs
      (throw (ex-info (str "Invalid rule " name ". No RHS (missing =>?).")
                      {})))
    `(def ~(vary-meta name assoc :rule true :doc doc)
       (cond-> ~(dsl/parse-rule* lhs rhs properties {})
           ~name (assoc :name ~(str (clojure.core/name (ns-name *ns*)) "/" (clojure.core/name name)))
           ~doc (assoc :doc ~doc)))))

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
       (cond-> ~(dsl/parse-query* binding definition {})
               ~name (assoc :name ~(str (clojure.core/name (ns-name *ns*)) "/" (clojure.core/name name)))
                ~doc (assoc :doc ~doc)))))
