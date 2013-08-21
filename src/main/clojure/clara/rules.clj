(ns clara.rules
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem])
  (:refer-clojure :exclude [==])
  (import [clara.rules.engine LocalTransport]))

(defn mk-rulebase 
  "Creates an empty rulebase."
  []
  (eng/->Rulebase {} [] [] [] [] {} {} {}))

(defn insert
  "Inserts one or more facts into a working session."
  [session & facts] 
  (eng/insert session facts))

(defn retract
  "Retracts a fact from a working session."
  [session & facts] 
  (eng/retract session facts))

(defn fire-rules 
  "Fires are rules in the given session."
  [session]
  (eng/fire-rules session))


(defn query 
  "Runs the given query with the given params against the session."
  [session query params]
  (eng/query session query params))

(defmacro == 
  "Unifies a variable with a given value."
  [variable content]
  `(do (assoc! ~'?__bindings__ ~(keyword variable) ~content)
       ~content)) ;; TODO: This might be better to use a dynamic var to create bindings.

(defn insert! 
  "To be executed from with a rule's right-hand side, this inserts a new fact or facts into working memory.
   Inserted facts are always logical, in that if the support for the insertion is removed, the fact
   will automatically be retracted."
  [& facts]
  (let [{:keys [rulebase transient-memory transport insertions]} eng/*current-session*
        {:keys [node token]} eng/*rule-context*]

    ;; Update the insertion count.
    (swap! insertions + (count facts))

    (doseq [[cls fact-group] (group-by class facts) 
            root (get-in rulebase [:alpha-roots cls])]

      ;; Track this insertion in our transient memory so logical retractions will remove it.
      (mem/add-insertions! transient-memory node token facts)
      (eng/alpha-activate root fact-group transient-memory transport))))


(defmacro mk-query
  "Contains a new query based on a sequence of a conditions."
  [params lhs]
  ;; TODO: validate params exist as keyworks in the query.
  `(eng/->Query
    ~params
    ~(eng/parse-lhs lhs)
    ~(eng/variables-as-keywords lhs)))

(defmacro mk-rule
  "Contains a new rule based on a sequence of a conditions and a righthand side."
  [lhs rhs]
  `(eng/->Production 
    ~(eng/parse-lhs lhs)
    ~(eng/compile-action (eng/variables-as-keywords lhs) rhs)))

(defn accumulate [& {:keys [initial-value reduce-fn combine-fn convert-return-fn] :as args}]
  (eng/map->Accumulator (if (:convert-return-fn args) 
                          args
                          (assoc args :convert-return-fn identity ))))

(defn add-rule
  "Returns a new rulebase identical to the given one, 
   but with the additional rules."
  ([rulebase] rulebase)
  ([rulebase production]
     (eng/add-production* rulebase production 
                          (eng/->ProductionNode production (:rhs production))))
  ([rulebase production & more]
     (add-rule (add-rule rulebase more) production)))

(defn add-query
  "Returns a new rulebase identical to the given one, 
   but with the additional queries."
  ([rulebase] rulebase)
  ([rulebase query]
     (eng/add-production* rulebase query 
                          (eng/->QueryNode query (:params query))))
  ([rulebase query & more]
     (add-query (add-query rulebase more) query)))

(defn mk-session 
  "Creates a new session using the given rule source."
  ([source & more]
     ;; Merge all of the sources together and create a session.
     (let [rulebase (eng/load-rules source)
           transport (LocalTransport.)
           
           ;; Merge other rule sessions into one.
           merged-rules 
           (reduce           
            (fn [rulebase other-source]
              (eng/conj-rulebases rulebase (eng/load-rules other-source)))
            (eng/load-rules source)
            more)]

       (eng/->LocalSession merged-rules (eng/local-memory merged-rules transport) transport))))

(defn- parse-rule-body [[head & more]]
  (cond
   ;; Detect the separator for the right-hand side.
   (= '=> head) {:lhs (list) :rhs (first more)}

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
   (symbol? head) (conj (parse-query-body (drop 2 more)) head)))

;; Treate a symbol as a rule source, loding all items in its namespace.
(extend-type clojure.lang.Symbol
  eng/IRuleSource
  (load-rules [sym]
    (reduce 
     (fn [rulebase item]
       (cond 
        (:rule (meta item))     
        (eng/add-production* rulebase @item 
                             (eng/->ProductionNode @item (:rhs @item)))
        (:query (meta item))  
        (eng/add-production* rulebase @item 
                             (eng/->QueryNode @item (:params @item)))
        :default rulebase))
     (mk-rulebase)
     (vals (ns-interns sym)))))

(defmacro defrule 
  "Defines a rule and stores it in the given var."
  [name & body]
  (let [doc (if (string? (first body)) (first body) nil)
        definition (if doc (rest body) body)
        {:keys [lhs rhs]} (parse-rule-body definition)]
    `(def ~(vary-meta name assoc :rule true :doc doc)
       (mk-rule ~lhs ~rhs))))

(defmacro defquery 
  "Defines a query and stored it in the given var."
  [name & body]
  (let [doc (if (string? (first body)) (first body) nil)
        binding (if doc (second body) (first body))
        definition (if doc (drop 2 body) (rest body) )]
    `(def ~(vary-meta name assoc :query true :doc doc) 
       (mk-query ~binding ~(parse-query-body definition)))))

