(ns clara.rules
  (:require [clara.rules.engine :as eng] [clara.rules.engine.state :as state]
            [clara.rules.engine.wme :as wme]))

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
   "
  [session query & params]
  (eng/query session query (apply hash-map params)))

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
  (eng/insert-facts! facts false))

(defn insert-all!
  "Behaves the same as insert!, but accepts a sequence of facts to be inserted. This can be simpler and more efficient for
   rules needing to insert multiple facts.

   See the doc in insert! for details on insert behavior.."
  [facts]
  (eng/insert-facts! facts false))

(defn insert-unconditional!
  "To be executed within a rule's right-hand side, this inserts a new fact or facts into working memory.

   This differs from insert! in that it is unconditional. The facts inserted will not be retracted
   even if the rule activation doing the insert becomes false.  Most users should prefer the simple insert!
   function as described above, but this function is available for use cases that don't wish to use
   Clara's truth maintenance."
  [& facts]
  (eng/insert-facts! facts true))

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
  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} state/*current-session*]

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
  (wme/map->Accumulator
   (merge
    {:combine-fn reduce-fn ; Default combine function is simply the reduce.
     :convert-return-fn identity ; Default conversion does nothing, so use identity.
     :retract-fn (fn [reduced retracted] reduced) ; Retractions do nothing by default.
     }
    args)))
