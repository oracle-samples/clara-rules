(ns clara.rete 
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [clojure.set :as s])
  (:refer-clojure :exclude [==]))

(defrecord Condition [type constraints binding-keys activate-fn])

(defrecord Production [lhs rhs])

(defrecord Query [lhs binding-keys])

(defrecord Network [alpha-roots beta-roots production-nodes query-nodes])

(defrecord Session [network memory])

(defrecord Token [facts bindings])

;; Token with no bindings, used as the root of beta nodes.
(def empty-token (->Token [] {}))

(defprotocol ILeftActivate
  (left-activate [node token memory]))

(defprotocol IRightActivate
  (right-activate [node fact bindings memory]))

(defprotocol ITransientMemory
  (get-beta-memory [memory node])
  (get-alpha-memory [memory node]))

(deftype TransientMemory [alpha-memory beta-memory]
  ITransientMemory
  (get-beta-memory [memory node]
    (or (get beta-memory node)
        (let [node-memory (transient {})]
          (assoc! beta-memory node node-memory)
          node-memory)))
  (get-alpha-memory [memory node]
    (or (get alpha-memory node)
        (let [node-memory (transient {})]
          (assoc! alpha-memory node node-memory)
          node-memory))))


(defn get-facts 
  "Returns a seq of [fact, fact-bindings] tuples"
  [memory node bindings]
  (or (get (get-alpha-memory memory node) bindings)
      []))

(defn add-fact [memory node node-bindings fact fact-bindings]
  (let [alpha-memory (get-alpha-memory memory node)
        current-facts (get alpha-memory node-bindings)]
    (assoc! alpha-memory node-bindings (conj current-facts [fact fact-bindings]))))

(defn get-tokens [memory node bindings]
  (or (get (get-beta-memory memory node) bindings)
      []))

(defn add-token [memory node node-bindings token]
  (let [beta-memory (get-beta-memory memory node)
        current-tokens (get beta-memory node-bindings)]
    (assoc! beta-memory node-bindings (conj current-tokens token))))


(defrecord ProductionNode [production rhs]
  ILeftActivate
  (left-activate [node token memory] 
    (let [beta-memory (get-beta-memory memory node)]
      (assoc! beta-memory :tokens (conj (get beta-memory :tokens) token)))))

(defrecord AlphaNode [condition children activation])

(defrecord JoinNode [condition children binding-keys]
  ILeftActivate
  (left-activate [node token memory] 
    ;; Add token to the node's working memory for future right activations.
    (let [bindings (:bindings token)
          node-bindings (select-keys bindings binding-keys)
          matched-facts (get-facts memory node node-bindings)]
      (add-token memory node node-bindings token)
      (doseq [[fact fact-binding] matched-facts 
              :let [child-token (->Token (conj (:facts token) fact) (conj bindings (:bindings token)))]
              child children]
        ;; Create a new token containing all bindings and left-activate the child.
        (left-activate child child-token memory))))

  IRightActivate
  (right-activate [node fact bindings memory]   

    (let [node-bindings (select-keys bindings binding-keys)
          matched-tokens (get-tokens memory node node-bindings)]
      ;; Add fact to the node's working memory for future left activations.
      (add-fact memory node node-bindings fact bindings)
      ;; For each token that matched the fact, create a new token with the fact and it's
      ;; bindings and notify our children.
      (doseq [token matched-tokens 
              :let [child-token (->Token (conj (:facts token) fact) (conj (:bindings token) bindings))]
              child children]
        ;; Create a new token containing all bindings and left-activate the child.
        (left-activate child child-token memory)))))

(defn get-fields 
  "Returns a list of fields in the given class."
  [cls]
  (map :name 
       (filter #(and (:type %) 
                     (not (#{'__extmap '__meta} (:name %)))   
                     (not (:static (:flags %)))) 
               (:members (reflect/type-reflect cls)))))

(defmacro == [variable content]
  `(do (assoc! ~'?__bindings__ ~(keyword variable) ~content)
       ~content))

(defn compile-condition 
  "Returns a function definition that can be used in alpha nodes to test the condition."
  [type constraints]
  (let [fields (get-fields type)
        ;; Create an assignments vector for the let block.
        assignments (mapcat #(list 
                              % 
                              (list (symbol (str ".-" (name %))) 'this)) 
                            fields)]

    `(fn [ ~(with-meta 
              'this 
              {:tag (symbol (.getName type))})] ; Add type hint to avoid runtime refection.
       (let [~@assignments
             ~'?__bindings__ (transient {})]
         (if (and ~@constraints)
           (persistent! ~'?__bindings__)
           nil)))))

(defn- compile-action [binding-keys rhs]
  (let [assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)]
    `(fn [~'?__session__ ~'?__token__] 
       (let [~@assignments]
         ~rhs))))

(defn- variables-as-keywords
  "Returns symbols in the given s-expression that start with '?' as keywords"
  [expression]
  (into #{} (for [item (flatten expression) 
                  :when (and (symbol? item) 
                             (= \? (first (name item))))] 
              (keyword  item))))

(defn create-condition [condition]
  (let [type (first condition) 
        constraints (apply vector (rest condition))
        binding-keys (variables-as-keywords constraints)]
    `(->Condition ~(resolve type) '~constraints ~binding-keys ~(compile-condition (resolve type) constraints))))

(def operators #{'and 'or 'not})

(defn- parse-expression [expression]
  (if (operators (first expression))
    {:type (keyword (first expression)) 
     :content (apply vector (map parse-expression (rest expression)))}
    {:type :condition :content (create-condition expression)}))

(defn parse-lhs
  "Parse the left-hand side and returns an AST"
  [lhs] 
  (parse-expression 
   (if (operators (first lhs))
     lhs
     (cons 'and lhs)))) ; "and" is implied if a list of constraints are given without an operator.


(defmacro new-query
  "Contains a new query based on a sequence of a conditions."
  [lhs]
  `(->Query 
    ~(parse-lhs lhs)
    ~(variables-as-keywords lhs)))

(defmacro new-rule
  "Contains a new rule based on a sequence of a conditions and a righthand side."
  [lhs rhs]
  `(->Production 
    ~(parse-lhs lhs)
    ~(compile-action (variables-as-keywords lhs) rhs)))

(defn- create-alpha-node [condition children]
  (->AlphaNode condition 
               children 
               (:activate-fn condition)))

(defn- create-production-node [production]
  (->ProductionNode production 
                    (:rhs production)))

(defn rete-network 
  "Creates an empty rete network."
  []
  (->Network {} [] [] {}))

(defn- create-join-node [condition children binding-keys]
  (->JoinNode condition children binding-keys))


(defn ast-to-dnf 
  "Convert an AST to disjunctive normal form."
  [ast] 

  (if (= :condition (:type ast))
    ;; Individual conditions can return unchanged.
    ast
   
    ;; Process children.
    (let [children (map ast-to-dnf (:content ast))
          conjunctions (filter #(#{:and :condition} (:type %)) children)
          ;; Merge all child disjunctions into a single list.
          disjunctions (mapcat :content (filter #(#{:or} (:type %)) children))]
      ;; TODO: Nodes with only a single expression as a child can be flattened.      
      (condp = (:type ast)

        :and
        (if (= 0 (count disjunctions))
          ;; If there are no disjunctions in the children, no changes are needed.
          ast
          ;; Distribute each disjunction over all conjunctions to
          ;; create a top-level disjunctive normal forum.
          {:type :or 
           :content (into [] (for [disjunction disjunctions]
                               {:type :and
                                :content (apply vector (cons disjunction conjunctions))}))})

        :or
        {:type :or
         ;; Nested disjunctions can be merged into the parent disjunction. We
         ;; the simply append nested conjunctions to create our DNF.
         :content (apply vector (concat disjunctions conjunctions))  }
        :not 
        (throw (RuntimeException. "TODO: use de Morgan's law to handle negations"))))))

(defn- add-rule* 
  "Adds a new production, returning a tuple of a new beta root and a new set of alpha roots"
  [[expression & more] production-node alpha-roots ancestor-binding-keys]
  (condp = (:type expression)
    
    ;; Recursively create children, then create a new join and alpha node for the condition.
    :condition (let [condition (:content expression)
                     node-binding-keys (s/intersection ancestor-binding-keys (:binding-keys condition))
                     all-binding-keys (s/union ancestor-binding-keys (:binding-keys condition))
                     [child new-alphas] (add-rule* more production-node alpha-roots all-binding-keys)
                     join-node (create-join-node condition [child] node-binding-keys)
                     alpha-node (create-alpha-node condition [join-node])]
      
                 [join-node, 
                  (merge-with concat new-alphas {(get-in alpha-node [:condition :type]) [alpha-node]})])
    
    ;; It's a conjunction, so add all content of the conjunction.
    :and (add-rule* (:content expression) production-node alpha-roots ancestor-binding-keys)

    ;; No more conditions, so terminate the recursion by returning the production node.
    ;; The returned production node will be the child of a join node built as we
    ;; work our way back up the recursion stack.
    nil [production-node alpha-roots]))

(defn- add-production* 
  [network production]

  (let [production-node (create-production-node production)
        dnf (ast-to-dnf (:lhs production))
        ;; Get a list of disjunctions, which may be a single item.
        disjunctions (if (= :or (:type dnf)) (:content dnf) [dnf])
        [alpha-roots beta-roots] 
        (loop [alpha-roots (:alpha-roots network)
               beta-roots (:beta-roots network)
               disjunctions disjunctions]
          (if (seq disjunctions)
            (let [[beta-root alpha-roots] (add-rule*  [(first disjunctions)] ; FIXME: should accept simple conjuection, not a vec.
                                                     production-node
                                                     alpha-roots
                                                     #{})]
              (recur alpha-roots (conj beta-roots beta-root) (rest disjunctions)))
            [alpha-roots beta-roots]))]

     (if (:rhs production)
        (->Network alpha-roots 
                   beta-roots 
                   (conj (:production-nodes network) production-node) 
                   (:query-nodes network))
        (->Network alpha-roots 
                   beta-roots 
                   (:production-nodes network) 
                   (assoc (:query-nodes network) production production-node)))))

(defn add-rule
  "Returns a new rete network identical to the given one, 
   but with the additional production."
  [network production]
  (add-production* network production))

(defn add-query
  "Returns a new rete network identical to the given one, 
   but with the additional query."
  [network query]
  (add-production* network query))



(defn create-working-memory 
  "Create a new working memory for the given network."
  [rete-network]
  ;; Creat a new memory and initialize it with 
  ;; dummy nodes for the beta roots.
  (reduce (fn [memory beta-node] 
            (left-activate beta-node empty-token memory)
            memory)
          (->TransientMemory 
           (transient {}) 
           (transient {}))
          (:beta-roots rete-network)))

(defn new-session 
  "Creates a new session using the given rete network."
  [rete-network]
  (->Session rete-network (create-working-memory rete-network)))

(defn activate-alpha
  "Activate an alpha node."
  [node fact memory] 
  (if-let [bindings ((:activation node) fact)]
    (doall 
     (map
      #(right-activate % fact bindings memory)
      (:children node)))))

(defn insert 
  "Insert a fact into the sesion"
  [session fact]
  ;; Locate the alpha roots that accept the given fact type.
  (if-let [roots (get-in session [:network :alpha-roots (class fact)])]
    (doall
     (map 
      #(activate-alpha % fact (:memory session))
      roots)))
  session)

(defn fire-rules [session]
  (doseq [node (get-in session [:network :production-nodes])
          token (:tokens (get-beta-memory (:memory session) node))]
    ((:rhs node) session token)))

(defn query 
  "Run a query against a working session. The given query must
   have been defined and added to the network."
  [session query]
  (let [query-node (get-in session [:network :query-nodes query])
        beta-memory (get-beta-memory (:memory session) query-node)]
    (map :bindings (:tokens beta-memory))))

;; TODO: compile alpha and beta nodes with functions that accept a "working memory" object
;; that is used for all references. (Kind of cool that working memory is fully isolated from rete...)
