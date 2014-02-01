(ns clara.tools.viz
  (use clara.rules)
  (require [dorothy.core :as dot]
           [clara.rules.schema :as schema]
           [clara.rules.engine :as eng]
           [hiccup.core :as h]
           [clara.rules.compiler :as com]
           [clojure.string :as string]))

(defn get-productions 
  "Returns a sequence of productions from the given sources."
  [sources]
  (mapcat
   #(if (satisfies? com/IRuleSource %)
      (com/load-rules %)
      %)
   sources))

(defn- condition-to-html 
  "Returns an HTML-based description of the given condition. "
  [condition]
  (h/html 
   (if (:accumulator condition)

     ;; Accumulator
     [:table  {:border "0" :cellborder "0"}
      [:tr [:td (h/h (str (:accumulator condition)))] [:td (str "into " (:result-binding condition))]  ]
      [:tr [:td [:b (str "from: "(last (string/split (.getName (get-in condition [:from :type])) #"\.")))]]]
      [:tr [:td (h/h (str (get-in condition [:from :constraints])))]]]

     ;; Handle as a normal, non-accumulator
     [:table  {:border "0" :cellborder "0"}
      (when (:type condition) 
        [:tr [:td [:b (str (last (string/split (.getName (:type condition)) #"\.")))]]])
      [:tr [:td (h/h (str (:constraints condition)))]]])))

(defn- rhs-to-html
  "Returns an HTML-based description of the right-hand side of a production.."
  [{:keys [name rhs] :as production}]
 (h/html [:table  {:border "0" :cellborder "0"}
                                 [:tr [:td name]]
                                 [:tr [:td (h/h (str rhs))]]]))

(defn show-network! 
  "Opens a window that contains a visualization of the Rete network associated with the rules."
  [& sources]
  (let [productions (get-productions sources)
        beta-tree (com/to-beta-tree productions)
        beta-nodes (for [beta-root beta-tree
                         beta-node (tree-seq :children :children beta-root)]
                     beta-node)
        alpha-nodes (com/to-alpha-tree beta-tree)]
    
    (-> 

     ;; Concat all of the components of our grab.
     (concat 
      ;; Graph beta edges.
      (for [beta-node beta-nodes
            child (:children beta-node)]
        [(:id beta-node) (:id child)])
      
      ;; Graph beta nodes.
      (for [beta-node beta-nodes]
        [(:id beta-node)
         {:shape
          (case (:node-type beta-node)
            :join :diamond
            :production :rectangle
            :query :parallelogram
            :Mrect
            )
          :label
          (case (:node-type beta-node)
            :join (str "JOIN ON: " (:join-bindings beta-node))
            :production (rhs-to-html (:production beta-node))
            :query (str (:name (:query beta-node)))
            :accumulator (h/h (str "ACCUMULATE: " (:accumulator beta-node)))
            :test (h/h (str "TEST: "(:condition beta-node)))
            :negation "NEGATION"
            )}])

      ;; Graph alpha edges.
      (for [alpha-node alpha-nodes
            child (:beta-children alpha-node)]
        [(hash alpha-node) child])

      ;; Graph alpha nodes.
      (for [alpha-node alpha-nodes
            child (:beta-children alpha-node)]
        [(hash alpha-node)
         {:shape :rectangle
          :label (condition-to-html (:condition alpha-node))}]))

     vec ; dorothy assumes a vector.
     dot/digraph
     dot/dot
     dot/show!)))

(def ^:private operators #{:and :or :not})

(defn- condition-to-id-map
  "Returns a map associating conditions to node ids"
  [production]

  ;; Recursively walk nested condition and return a map associating each with a unique id.
  (let [conditions (tree-seq #(operators (schema/condition-type %)) 
                            #(rest %)
                            (if (= 1 (count (:lhs production)))
                              (first (:lhs production))
                              (into [:and] (:lhs production))))]  ; Add implied and for all conditions.

    (into 
     {}
     (map (fn [condition index]
            [condition (str index "-" (hash production))])
          conditions
          (range)))))

(defn- production-to-dot [production]
  (let [condition-to-ids (condition-to-id-map production) 

        ;; Identify root conditions by first finding all children,
        ;; and removing them from the set of all conditions.
        child-conditions (into 
                          #{}
                          (for [[condition id] condition-to-ids
                                :when #(operators (schema/condition-type condition)) 
                                child (rest condition)]
                            child))
        root-conditions (for [[condition id] condition-to-ids
                              :when (not (child-conditions condition))]
                          [condition id])]

    (concat

     ;; Graph condtions.
     (for [[condition id] condition-to-ids
           :let [condition-type (schema/condition-type condition)]]
       [id
        (if (operators condition-type)
          {:shape :diamond
           :label (name condition-type)}
          {:shape :ellipse
           :label (condition-to-html condition)})])

     ;; Graph condition relationships, with children pointing to parents.
     (for [[condition id] condition-to-ids
           :when (operators (schema/condition-type condition))
           child (rest condition)]
       [(get condition-to-ids child) id]
       )

     ;; Graph the production itself.
     [[(hash production) {:shape :rectangle
                          :style :bold
                          :label (rhs-to-html production)}]]

     ;; Graph the edge of root conditions to the production.
     (for [[condition id] root-conditions]
       [id (hash production)]))))

(defn- get-insertions 
  "Returns the insertions done by a production."
  [production]
  (if-let [rhs (:rhs production)]

    (into
     #{}
     (for [expression (tree-seq seq? identity rhs)
           :when (and (list? expression)
                      (= 'clara.rules/insert! (first expression))) ; Find insert! calls
           [create-fact-fn create-fact-args] (rest expression)
           :when (re-matches #"->.*" (name create-fact-fn))] ; Find record constructors.

       (str (namespace create-fact-fn)
            "."
            (subs (name create-fact-fn) 2)))) ; Return the record type.
    #{}))

(defn- get-uses
  "Returns the facts consumed by a production."
  [production]
  (into #{}
        (for [[condition id] (condition-to-id-map production)
              :when (:type condition)]
          (.getName (:type condition)))))

(defn- insertions-to-dot [productions]
  (let [types-to-ids ; A map of all conditions and their node ids.
        (reduce
         (fn [map [type id]]
           (update-in map [(.getName type)] conj id))
         {}
         (for [production productions
               [condition id] (condition-to-id-map production)
               :when (:type condition)]
           [(:type condition) id]))

        productions-to-insertions ; Map asociating productions with each insertion.
        (into {}
              (for [production productions]
                [production (get-insertions production)]))
        ]

    ;; For each production, lookup nodes that match what it inserts
    ;; and create a tuple for it.
    (for [[production insertions] productions-to-insertions
          insertion insertions
          condition-id (get types-to-ids insertion)]
      [(hash production) condition-id {:style :dashed}])))

(defn inserts? 
  "Predicate that returns true when the given rule inserts the given fact"
  [rule fact]
  (contains? (get-insertions rule)
             (.getName fact)))

(defn uses?
  "Predicate that returns true when the given rule uses the given fact."
  [rule fact]
  (contains? (get-uses rule)
             (.getName fact)))

(defn logic-to-dot
  [sources]
  (let [productions (get-productions sources)]
    
    (->
     (concat
      (mapcat production-to-dot productions)
      (insertions-to-dot productions))
     vec ; dorothy assumes a vector.
     dot/digraph 
     dot/dot)))

(defn save-png! 
  [destination & sources]
  (dot/save! (logic-to-dot sources) destination {:format :png}))

(defn show-logic! 
  [& sources]
  (dot/show! (logic-to-dot sources)))