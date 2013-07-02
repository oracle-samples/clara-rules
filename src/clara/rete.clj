(ns clara.rete 
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [clojure.set :as s])
  (:refer-clojure :exclude [==]))

(defrecord Condition [type constraints binding-keys activate-fn])

(defrecord Production [lhs rhs])

(defrecord Query [lhs binding-keys])

(defrecord Network [alpha-roots beta-roots production-nodes query-nodes])

(defrecord Token [facts bindings])
 
;; Token with no bindings, used as the root of beta nodes.
(def empty-token (->Token [] {}))

;; Returns a new session with the additional facts inserted.
(defprotocol IWorkingMemory
  (insert [session fact])
  (retract [session fact])
  ;; Fires pending rules and returns a new session where they are in a fired state.
  (fire-rules [session])
  (query [session query]))

;; Left activation protocol for various types of beta nodes.
(defprotocol ILeftActivate
  (left-activate [node token memory transport])
  (left-retract [node token memory transport]))

;; Right activation protocol to insert new facts, connect alpha nodes,
;; and beta nodes.
(defprotocol IRightActivate
  (right-activate [node fact bindings memory transport])
  (right-retract [node fact bindings memory transport]))

;; The transport protocol for sending and retracting items between nodes.
(defprotocol ITransport
  (send-fact [transport node fact bindings])
  (send-token [transport node token])
  (retract-fact [transport node fact bindings])
  (retract-token [transport node token]))

;; Simple, in-memory transport.
(deftype LocalTransport [memory]
  ITransport
  (send-fact [transport node fact bindings]
    (right-activate node fact bindings memory transport))
  (send-token [transport node token]
    (left-activate node token memory transport))
  (retract-fact [transport node fact bindings]
    (right-retract node fact bindings memory transport))
  (retract-token [transport node token]
    (left-retract node token memory transport)))

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

(defn- add-fact 
  "Helper function to add a fact to the alpha memory of a node."
  [memory node node-bindings fact fact-bindings]
  (let [alpha-memory (get-alpha-memory memory node)
        current-facts (get alpha-memory node-bindings)]
    (assoc! alpha-memory node-bindings (conj current-facts [fact fact-bindings]))))

(defn- remove-fact
  "Remove a fact from the node's memory. Returns true if it was removed, false otherwise"
  [memory node fact node-bindings]
  (let [alpha-memory (get-alpha-memory memory node)
        current-facts (get alpha-memory node-bindings)
        filtered-facts (filter (fn [[candidate candidate-bindings]] (not= fact candidate)) current-facts)]
    
    ;; Update our memory with the changed facts.
    (assoc! alpha-memory node-bindings filtered-facts)
    ;; If the count of facts changed, we removed something.
    (not= (count current-facts) (count filtered-facts))))

(defn- get-tokens 
  "Returns a seq of the tokens that match the given bindings for the node."
  [memory node bindings]
  (or (get (get-beta-memory memory node) bindings)
      []))

(defn- add-token 
  "Adds a token to the beta memory for a node."
  [memory node node-bindings token]
  (let [beta-memory (get-beta-memory memory node)
        current-tokens (get beta-memory node-bindings)]
    (assoc! beta-memory node-bindings (conj current-tokens token))))


(defn- remove-token
  "Removes a token from the beta memory for a node. Returns true if it was removed, false wotherwise"
  [memory node node-bindings token]
  (let [beta-memory (get-beta-memory memory node)
        current-tokens (get beta-memory node-bindings)
        filtered-tokens (filter #(not= % token) current-tokens)]
    (assoc! beta-memory node-bindings filtered-tokens)
    ;; If the count of tokens changed, we remove something.
    (not= (count current-tokens) (count filtered-tokens))))

(defrecord ProductionNode [production rhs]
  ILeftActivate
  (left-activate [node token memory transport] 
    (let [beta-memory (get-beta-memory memory node)]
      (assoc! beta-memory :tokens (conj (get beta-memory :tokens) token))))
  (left-retract [node token memory transport] 
    (let [beta-memory (get-beta-memory memory node)]
      (assoc! beta-memory :tokens (filter #(not= % token) (get beta-memory :tokens))))))

(defrecord AlphaNode [condition children activation]
  IRightActivate
  (right-activate [node fact bindings memory transport]
    (if-let [bindings (activation fact)]
      (doall 
       (map
        #(send-fact transport % fact bindings)
        children))))
  (right-retract [node fact bindings memory transport]
    (if-let [bindings (activation fact)]
      (doall 
       (map
        #(retract-fact transport % fact bindings)
        children)))))

(defrecord JoinNode [condition children binding-keys]
  ILeftActivate
  (left-activate [node token memory transport] 
    ;; Add token to the node's working memory for future right activations.
    (let [bindings (:bindings token)
          node-bindings (select-keys bindings binding-keys)
          matched-facts (get-facts memory node node-bindings)]
      (add-token memory node node-bindings token)
      (doseq [[fact fact-binding] matched-facts 
              :let [child-token (->Token (conj (:facts token) fact) (conj bindings (:bindings token)))]
              child children]
        ;; Create a new token containing all bindings and left-activate the child.
        (send-token transport child child-token))))
  (left-retract [node token memory transport] 
    (let [bindings (:bindings token)
          node-bindings (select-keys bindings binding-keys)]
      (when (remove-token memory node node-bindings token)
        (doseq [[fact fact-binding] (get-facts memory node node-bindings)
                :let [child-token (->Token (conj (:facts token) fact) (conj bindings (:bindings token)))]
                child children]
          ;; Retract the token from all children.
          (retract-token transport child child-token)))))

  IRightActivate
  (right-activate [node fact bindings memory transport]   

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
        (send-token transport child child-token))))
  (right-retract [node fact bindings memory transport]   
    ;; TODO: find all tokens that match the retracted fact, and retract their children.
    ;; Then remove the fact from memory.
    (let [node-bindings (select-keys bindings binding-keys)]
      (when (remove-fact  memory node fact node-bindings)
        (doseq [token (get-tokens memory node node-bindings)
              :let [child-token (->Token (conj (:facts token) fact) (conj (:bindings token) bindings))]
              child children]
          ;; Retract children impacted by the retracted fact.
          (retract-token transport child child-token))))))

(defrecord NegationNode [condition children binding-keys]
  ILeftActivate
  (left-activate [node token memory transport]
    ;; Add token to the node's working memory for future right activations.
    (let [bindings (:bindings token)
          node-bindings (select-keys bindings binding-keys)
          matched-facts (get-facts memory node node-bindings)]
      (add-token memory node node-bindings token)
      (when (empty? matched-facts)
        (doseq [child children]
          (send-token transport child token)))))

  (left-retract [node token memory transport]
    (let [bindings (:bindings token)
          node-bindings (select-keys bindings binding-keys)]
      ;; If the token exists and doesn't match any facts, retract it
      ;; from all children as well.
      (when (and (remove-token memory node node-bindings token)
                 (empty? (get-facts memory node node-bindings)))
        (doseq [child children]
          (retract-token transport child token)))))

  IRightActivate
  (right-activate [node fact bindings memory transport]
    (let [node-bindings (select-keys bindings binding-keys)
          matched-tokens (get-tokens memory node node-bindings)]
      ;; Add fact to the node's working memory for future left activations.
      (add-fact memory node node-bindings fact bindings)
      ;; A negation was hit, so retract each matching token from the children nodes.
      (doseq [token matched-tokens 
              child children]
        ;; Create a new token containing all bindings and left-activate the child.
        (retract-token transport child token))))

  (right-retract [node fact bindings memory transport]   
    (let [node-bindings (select-keys bindings binding-keys)]
      (when (remove-fact  memory node fact node-bindings)
        (doseq [token (get-tokens memory node node-bindings)
              child children]
          ;; The retracted fact removed the negation, so send it to the children.
          (send-token transport child token))))))

(defn- get-fields 
  "Returns a list of fields in the given class."
  ;; TODO: add support for java beans.
  ;; TODO: consider dynamic support for simple maps.
  [cls]
  (map :name 
       (filter #(and (:type %) 
                     (not (#{'__extmap '__meta} (:name %)))   
                     (not (:static (:flags %)))) 
               (:members (reflect/type-reflect cls)))))

(defmacro == 
  "Unifies a variable with a given value."
  [variable content]
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

(defn- parse-lhs
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

(defn rete-network 
  "Creates an empty rete network."
  []
  (->Network {} [] [] {}))

(defn ast-to-dnf 
  "Convert an AST to disjunctive normal form."
  [ast] 
  (condp = (:type ast)
    ;; Individual conditions can return unchanged.
    :condition
    ast

    ;; Apply de Morgan's law to push negation nodes to the leaves. 
    :not
    (let [children (:content ast)
          child (first children)
          ;; Function to create negated grandchildren for de Morgan's law.
          negate-grandchildren #(into [] (for [grandchild (:content child)] 
                                           {:type :not
                                            :content [grandchild]}))]

      (when (not= 1 (count children))
        (throw (IllegalArgumentException. "Negation must have only one child.")))

      (condp = (:type child)

        ;; If the child is a single condition, simply return the ast.
        :condition ast

        :and (ast-to-dnf {:type :or
                          :content (negate-grandchildren)})
        
        :or (ast-to-dnf {:type :and 
                         :content (negate-grandchildren)})))
   
    ;; For all others, recursively process the children.
    (let [children (map ast-to-dnf (:content ast))
          ;; Get all conjunctions, which will not conain any disjunctions since they were processed above.
          conjunctions (filter #(#{:and :condition :not} (:type %)) children)
          ;; Merge all child disjunctions into a single list.
          disjunctions (mapcat :content (filter #(#{:or} (:type %)) children))]
      ;; TODO: Nodes with only a single expression as a child can be flattened.      
      (condp = (:type ast)

        :and
        (if (= 0 (count disjunctions))
          
          ;; If there are no disjunctions in the processed children, no further changes are needed.
          {:type :and
           :content (apply vector children)}
          
          ;; The children had disjunctions, so distribute them to convert to DNF.
          {:type :or 
           :content (into [] (for [disjunction disjunctions]
                               {:type :and
                                :content (apply vector (cons disjunction conjunctions))}))})

        :or
        {:type :or
         ;; Nested disjunctions can be merged into the parent disjunction. We
         ;; the simply append nested conjunctions to create our DNF.
         :content (apply vector (concat disjunctions conjunctions))  }))))

(defn- add-rule* 
  "Adds a new production, returning a tuple of a new beta root and a new set of alpha roots"
  [[expression & more] production-node alpha-roots ancestor-binding-keys]
  (condp = (:type expression)
    
    ;; Recursively create children, then create a new join and alpha node for the condition.
    :condition 
    (let [condition (:content expression)
          node-binding-keys (s/intersection ancestor-binding-keys (:binding-keys condition))
          all-binding-keys (s/union ancestor-binding-keys (:binding-keys condition))
          [child new-alphas] (add-rule* more production-node alpha-roots all-binding-keys)
          join-node  (->JoinNode condition [child] node-binding-keys)
          alpha-node (->AlphaNode condition 
                                  [join-node] 
                                  (:activate-fn condition))]
      
      [join-node, 
       (merge-with concat new-alphas {(get-in alpha-node [:condition :type]) [alpha-node]})])
    
    ;; It's a conjunction, so add all content of the conjunction.
    :and 
    (add-rule* (:content expression) production-node alpha-roots ancestor-binding-keys)

    :not 
    (let [condition (:content (first (:content expression))) ; Get the child content of the not clause.
          node-binding-keys (s/intersection ancestor-binding-keys (:binding-keys condition))
          all-binding-keys (s/union ancestor-binding-keys (:binding-keys condition))
          [child new-alphas] (add-rule* more production-node alpha-roots all-binding-keys)
          join-node  (->NegationNode condition [child] node-binding-keys)
          alpha-node (->AlphaNode condition 
                                  [join-node] 
                                  (:activate-fn condition))]
      
      [join-node, 
       (merge-with concat new-alphas {(get-in alpha-node [:condition :type]) [alpha-node]})])

    ;; No more conditions, so terminate the recursion by returning the production node.
    ;; The returned production node will be the child of a join node built as we
    ;; work our way back up the recursion stack.
    nil [production-node alpha-roots]))

(defn- add-production* 
  [network production]

  (let [production-node (->ProductionNode production (:rhs production)) 
        dnf (ast-to-dnf (:lhs production))
        ;; Get a list of disjunctions, which may be a single item.
        disjunctions (if (= :or (:type dnf)) (:content dnf) [dnf])
        [alpha-roots beta-roots] 
        (loop [alpha-roots (:alpha-roots network)
               beta-roots (:beta-roots network)
               disjunctions disjunctions]
          (if (seq disjunctions)
            (let [[beta-root alpha-roots] (add-rule*  [(first disjunctions)] ; FIXME: should accept simple conjunction, not a vec.
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


(defrecord SimpleWorkingMemory [network memory transport]
  IWorkingMemory
  (insert [session fact]
    (if-let [roots (get-in network [:alpha-roots (class fact)])]
      (doall
       (map 
        #(send-fact transport % fact {})
        roots)))
    session) ;; FIXME: return a new session that is persistent.
  (retract [session fact]
    (if-let [roots (get-in network [:alpha-roots (class fact)])]
      (doall
       (map 
        #(retract-fact transport % fact {})
        roots)))
    session)
  (fire-rules [session]
    (doseq [node (get-in session [:network :production-nodes])
            token (:tokens (get-beta-memory (:memory session) node))]
      ((:rhs node) session token)))
  (query [session query]
    (let [query-node (get-in network [:query-nodes query])
          beta-memory (get-beta-memory memory query-node)]
      (map :bindings (:tokens beta-memory)))))

(defn new-session 
  "Creates a new session using the given rete network."
  [rete-network]
  (let [memory (->TransientMemory 
                (transient {}) 
                (transient {})) 
        session (->SimpleWorkingMemory rete-network memory (LocalTransport. memory))]

    ;; Activate the beta roots.
    (doseq [beta-node (:beta-roots rete-network)]
      (left-activate beta-node empty-token memory (:transport session)))
    session))
