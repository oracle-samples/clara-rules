(ns clara.rules
  (:require [clara.rules.engine :as eng]
            [clara.rules.memory :as mem])
  (:refer-clojure :exclude [==])
  (import [clara.rules.engine LocalTransport]))

(defn rete-network 
  "Creates an empty rete network."
  []
  (eng/->Network {} [] [] {}))

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
  (let [{:keys [network transient-memory transport]} eng/*current-session*
        {:keys [node token]} eng/*rule-context*]
    (doseq [[cls fact-group] (group-by class facts) 
            root (get-in network [:alpha-roots cls])]

      ;; Track this insertion in our transient memory so logical retractions will remove it.
      (mem/add-insertions! transient-memory node token facts)
      (eng/alpha-activate root fact-group transient-memory transport))))


(defmacro new-query
  "Contains a new query based on a sequence of a conditions."
  [params lhs]
  ;; TODO: validate params exist as keyworks in the query.
  `(eng/->Query
    ~params
    ~(eng/parse-lhs lhs)
    ~(eng/variables-as-keywords lhs)))

(defmacro new-rule
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
  "Returns a new rete network identical to the given one, 
   but with the additional rules."
  ([network] network)
  ([network production]
     (eng/add-production* network production 
                          (eng/->ProductionNode production (:rhs production))))
  ([network production & more]
     (add-rule (add-rule network more) production)))

(defn add-query
  "Returns a new rete network identical to the given one, 
   but with the additional queries."
  ([network] network)
  ([network query]
     (eng/add-production* network query 
                          (eng/->QueryNode query (:params query))))
  ([network query & more]
     (add-query (add-query network more) query)))

(defn new-session 
  "Creates a new session using the given rule source."
  [source]
  (let [rete-network (eng/load-rules source)
        memory (mem/to-transient (mem/local-memory rete-network))
        transport (LocalTransport.)]

    ;; Activate the beta roots.
    (doseq [beta-node (:beta-roots rete-network)]
      (eng/left-activate beta-node {} [eng/empty-token] memory transport))

    (eng/->LocalSession rete-network (mem/to-persistent! memory) transport)))

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

(extend-type clojure.lang.Symbol
  eng/IRuleLoader
  (load-rules [sym]
    (reduce 
     (fn [network item]
       (cond 
        (:rule (meta item))     
        (eng/add-production* network @item 
                             (eng/->ProductionNode @item (:rhs @item)))
        (:query (meta item))  
        (eng/add-production* network @item 
                             (eng/->QueryNode @item (:params @item)))
        :default network))
     (rete-network)
     (vals (ns-interns sym)))))

(defmacro defrule [name & body]
  (let [{:keys [lhs rhs]} (parse-rule-body body)]
    `(def ~(vary-meta name assoc :rule true) (new-rule ~lhs ~rhs))))

(defmacro defquery [name binding & body]
  `(def ~(vary-meta name assoc :query true) (new-query ~binding ~(parse-query-body body))))

