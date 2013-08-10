(ns clara.rules.engine
  (:use clara.rules.memory)
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [clojure.set :as s])
  (:refer-clojure :exclude [==]))

;; Protocol for loading rules from some arbitrary source.
(defprotocol IRuleSource
  (load-rules [source]))

(defrecord Condition [type constraints binding-keys activate-fn])

(defrecord Production [lhs rhs])

(defrecord Query [params lhs binding-keys])

(defrecord Rulebase [alpha-roots beta-roots production-nodes query-nodes node-to-id id-to-node]
  IRuleSource
  (load-rules [this] this)) ; A rulebase can be viewed as a rule loader; it simply loads itself.

(defrecord Token [facts bindings])

;; A working memory element
(defrecord Element [fact bindings])

;; Token with no bindings, used as the root of beta nodes.
(def empty-token (->Token [] {}))

;; Returns a new session with the additional facts inserted.
(defprotocol ISession
  (insert [session fact])
  (retract [session fact])
  ;; Fires pending rules and returns a new session where they are in a fired state.
  (fire-rules [session])
  (query [session query params]))

;; Left activation protocol for various types of beta nodes.
(defprotocol ILeftActivate
  (left-activate [node join-bindings tokens memory transport])
  (left-retract [node join-bindings tokens memory transport])
  (description [node])
  (get-join-keys [node]))

;; Right activation protocol to insert new facts, connect alpha nodes,
;; and beta nodes.
(defprotocol IRightActivate
  (right-activate [node join-bindings elements memory transport])
  (right-retract [node join-bindings elements memory transport]))

;; The transport protocol for sending and retracting items between nodes.
(defprotocol ITransport
  (send-elements [transport memory nodes elements])
  (send-tokens [transport memory nodes tokens])
  (retract-elements [transport memory nodes elements])
  (retract-tokens [transport memory nodes tokens]))

;; Enable transport tracing for debugging purposes.
(def ^:dynamic *trace-transport* false)

;; Simple, in-memory transport.
(deftype LocalTransport [] 
  ITransport
  (send-elements [transport memory nodes elements]
    (when (and *trace-transport* (seq elements)) 
      (println "ELEMENTS " elements " TO " (map description nodes)))
    (doseq [[bindings element-group] (group-by :bindings elements)
            node nodes]
      (right-activate node 
                      (select-keys bindings (get-join-keys node))
                      element-group 
                      memory 
                      transport)))

  (send-tokens [transport memory nodes tokens]
    (when (and *trace-transport* (seq tokens)) 
      (println "TOKENS " tokens " TO " (map description nodes)))
    (doseq [[bindings token-group] (group-by :bindings tokens)
            node nodes]
      (left-activate node 
                     (select-keys bindings (get-join-keys node))
                     token-group 
                     memory 
                     transport)))

  (retract-elements [transport memory nodes elements]
    (when (and *trace-transport* (seq elements)) 
      (println "RETRACT ELEMENTS " elements " TO " (map description nodes)))
    (doseq  [[bindings element-group] (group-by :bindings elements)
             node nodes]
      (right-retract node 
                     (select-keys bindings (get-join-keys node)) 
                     element-group
                     memory 
                     transport)))

  (retract-tokens [transport memory nodes tokens]
    (when (and *trace-transport* (seq tokens))
      (println "RETRACT TOKENS " tokens " TO " (map description nodes)))
    (doseq  [[bindings token-group] (group-by :bindings tokens)
             node nodes]
      (left-retract  node 
                     (select-keys bindings (get-join-keys node)) 
                     token-group
                     memory 
                     transport))))

(defprotocol IAlphaActivate 
  (alpha-activate [node facts memory transport])
  (alpha-retract [node facts memory transport]))

(defrecord ProductionNode [production rhs]
  ILeftActivate  
  (left-activate [node join-bindings tokens memory transport]
    ;; Tokens added to be pending rule execution.
    (add-tokens! memory node join-bindings tokens)) 

  (left-retract [node join-bindings tokens memory transport] 
    ;; Remove any tokens to avoid future rule execution on retracted items.
    (remove-tokens! memory node join-bindings tokens)

    ;; Unmark the node as fired for the given token, so future insertions will work.
    (doseq [token tokens]
      (unmark-as-fired! memory node token))

    ;; Retract any insertions that occurred due to the retracted token.
    (let [insertions (remove-insertions! memory node tokens)]
      (doseq [[cls fact-group] (group-by class insertions) 
              root (get-in (get-rulebase memory) [:alpha-roots cls])]
        (alpha-retract root fact-group memory transport))))

  (get-join-keys [node] [])

  (description [node] "Production"))

(defrecord QueryNode [query param-keys]
  ILeftActivate  
  (left-activate [node join-bindings tokens memory transport] 
    (add-tokens! memory node join-bindings tokens))

  (left-retract [node join-bindings tokens memory transport] 
    (remove-tokens! memory node join-bindings tokens))

  (get-join-keys [node] param-keys)

  (description [node] (str "Query of " (pr-str node))))

(defrecord AlphaNode [condition children activation]
  IAlphaActivate
  (alpha-activate [node facts memory transport]
    (send-elements
     transport memory children
     (for [fact facts
           :let [bindings (activation fact)] :when bindings]
       (->Element fact bindings))))

  (alpha-retract [node facts memory transport]

    (retract-elements
     transport memory children
     (for [fact facts
           :let [bindings (activation fact)] :when bindings]
       (->Element fact bindings)))))

(defrecord JoinNode [condition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport] 
    ;; Add token to the node's working memory for future right activations.
    (add-tokens! memory node join-bindings tokens)
    (send-tokens 
     transport 
     memory
     children
     (for [element (get-elements memory node join-bindings)
           token tokens
           :let [fact (:fact element)
                 fact-binding (:bindings element)]]
       (->Token (conj (:facts token) fact) (conj fact-binding (:bindings token))))))

  (left-retract [node join-bindings tokens memory transport] 
    (retract-tokens 
     transport 
     memory
     children 
     (for [token (remove-tokens! memory node join-bindings tokens)
           element (get-elements memory node join-bindings)
           :let [fact (:fact element)
                 fact-bindings (:bindings element)]]
       (->Token (conj (:facts token) fact) (conj fact-bindings (:bindings token))))))

  (get-join-keys [node] binding-keys)

  (description [node] (str "Join of " (:type condition) " constraints: " (:constraints condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport]         
    (add-elements! memory node join-bindings elements)
    (send-tokens 
     transport 
     memory
     children
     (for [token (get-tokens memory node join-bindings)
           {:keys [fact bindings] :as element} elements]
       (->Token (conj (:facts token) fact) (conj (:bindings token) bindings)))))

  (right-retract [node join-bindings elements memory transport]   
    (retract-tokens
     transport
     memory
     children
     (for [{:keys [fact bindings] :as element} (remove-elements! memory node elements join-bindings)
           token (get-tokens memory node join-bindings)]
       (->Token (conj (:facts token) fact) (conj (:bindings token) bindings))))))

(defrecord NegationNode [condition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport]
    ;; Add token to the node's working memory for future right activations.
    (add-tokens! memory node join-bindings tokens)
    (when (empty? (get-elements memory node join-bindings))
      (send-tokens transport memory children tokens)))

  (left-retract [node join-bindings tokens memory transport]
    (when (empty? (get-elements memory node join-bindings))
      (retract-tokens transport memory children tokens)))

  (get-join-keys [node] binding-keys)

  (description [node] "Negation")

  IRightActivate
  (right-activate [node join-bindings elements memory transport]
    (add-elements! memory node join-bindings elements)
    ;; Retract tokens that matched the activation, since they are no longer negatd.
    (retract-tokens transport memory children (get-tokens memory node join-bindings)))

  (right-retract [node join-bindings elements memory transport]   
    (remove-elements! memory node elements join-bindings) ;; FIXME: elements must be zero to retract.
    (send-tokens transport memory children (get-tokens memory node join-bindings))))

;;
(defrecord Accumulator [result-binding input-condition initial-value reduce-fn combine-fn convert-return-fn])

(defn- retract-accumulated [node accumulator token result fact-bindings transport memory]
  (let [converted-result ((get-in accumulator [:definition :convert-return-fn]) result)
        new-facts (conj (:facts token) converted-result)
        new-bindings (merge (:bindings token) 
                            fact-bindings
                            (when (:result-binding accumulator) 
                              { (:result-binding accumulator) 
                                converted-result}))] 

    (retract-tokens transport memory (:children node) 
                    [(->Token new-facts new-bindings)])))

(defn- send-accumulated [node accumulator token result fact-bindings transport memory]
  (let [converted-result ((get-in accumulator [:definition :convert-return-fn]) result)
        new-bindings (merge (:bindings token) 
                            fact-bindings
                            (when (:result-binding accumulator) 
                              { (:result-binding accumulator) 
                                converted-result}))]

    (send-tokens transport memory (:children node)
                 [(->Token (conj (:facts token) converted-result) new-bindings)])))

(defrecord AccumulateNode [accumulator definition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport] 
    (let [previous-results (get-accum-results memory node join-bindings)]
      (add-tokens! memory node join-bindings tokens)
      (doseq [token tokens
              [fact-bindings previous] previous-results]
        (send-accumulated node accumulator token previous fact-bindings transport memory))))

  (left-retract [node join-bindings tokens memory transport] 
    (let [previous-results (get-accum-results memory node join-bindings)]
      (doseq [token (remove-tokens! memory node join-bindings tokens)
              [fact-bindings previous] previous-results]
        (retract-accumulated node accumulator token previous fact-bindings transport memory))))

  (get-join-keys [node] binding-keys)

  (description [node] "Accumulate")

  IRightActivate
  (right-activate [node join-bindings elements memory transport]   
    (add-elements! memory node join-bindings elements)
    (doseq [:let [matched-tokens (get-tokens memory node join-bindings)]
            {:keys [fact bindings] :as element} elements
            :let [previous (get-accum-result memory node join-bindings bindings)]]

      ;; If the accumulation result was previously calculated, retract it
      ;; from the children.
      (when previous
        ;; TODO: reuse previous token rather than recreating it?
        (doseq [token (get-tokens memory node join-bindings)]
          (retract-accumulated node accumulator token previous bindings transport memory)))

      ;; Reduce to create a new token and send it downstream.
      (let [initial (:initial-value definition)
            reduce-fn (:reduce-fn definition)
            ;; Get the set of facts that are reducible.
            reducible-facts (map :fact (get-elements memory node join-bindings)) ;; Get only facts, not bindings...
            reduced (r/reduce reduce-fn initial reducible-facts)]

        (add-accum-result! memory node join-bindings reduced bindings)
        (doseq [token matched-tokens]
          (send-accumulated node accumulator token reduced bindings transport memory)))))

  (right-retract [node join-bindings elements memory transport]   
    
    (doseq [:let [matched-tokens (get-tokens memory node join-bindings)]
            {:keys [fact bindings] :as element} (remove-elements! memory node elements join-bindings)
            :let [previous (get-accum-result memory node join-bindings bindings)]]
      
      ;; If the accumulation result was previously calculated, retract it
      ;; from the children.
      
      (when previous
        ;; TODO: reuse previous token rather than recreating it?
        (doseq [token (get-tokens memory node join-bindings)]
          (retract-accumulated node accumulator token previous bindings transport memory)))

      ;; Reduce to create a new token and send it downstream.
      
      (let [initial (:initial-value definition)
            reduce-fn (:reduce-fn definition)
            ;; Get the set of facts that are reducible.
            reducible-facts (map :fact (get-elements memory node join-bindings)) ;; Get only facts
            reduced (r/reduce reduce-fn initial reducible-facts)]

        (add-accum-result! memory node join-bindings reduced bindings)
        (doseq [token matched-tokens]
          (send-accumulated node accumulator token reduced bindings transport memory))))))

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

(defn compile-condition 
  "Returns a function definition that can be used in alpha nodes to test the condition."
  [type constraints result-binding]
  (let [fields (get-fields type)
        ;; Create an assignments vector for the let block.
        assignments (mapcat #(list 
                              % 
                              (list (symbol (str ".-" (name %))) 'this)) 
                            fields)
        initial-bindings (if result-binding {result-binding 'this}  {})]

    `(fn [ ~(with-meta 
              'this 
              {:tag (symbol (.getName type))})] ; Add type hint to avoid runtime refection.
       (let [~@assignments
             ~'?__bindings__ (transient ~initial-bindings)]
         (if (and ~@constraints)
           (persistent! ~'?__bindings__)
           nil)))))

(defn compile-action [binding-keys rhs]
  (let [assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)]
    `(fn [~'?__session__ ~'?__token__] 
       (let [~@assignments]
         ~rhs))))

(defn variables-as-keywords
  "Returns symbols in the given s-expression that start with '?' as keywords"
  [expression]
  (into #{} (for [item (flatten expression) 
                  :when (and (symbol? item) 
                             (= \? (first (name item))))] 
              (keyword  item))))

(defrecord AccumulatorDef [initial-value reduce-fn combine-fn convert-return-fn])

(defn- construct-condition [condition result-binding]
  (let [type (first condition)
        constraints (apply vector (rest condition))
        binding-keys (variables-as-keywords constraints)]
    
    `(->Condition ~(resolve type) 
                  '~constraints ~binding-keys 
                  ~(compile-condition (resolve type) constraints result-binding))))

(defn create-condition [condition]
  ;; Grab the binding of the operation result, if present.
  (let [result-binding (if (= '<- (second condition)) (keyword (first condition)) nil)
        condition (if result-binding (drop 2 condition) condition)]

    (when (and (not= nil result-binding)
               (not= \? (first (name result-binding))))
      (throw (IllegalArgumentException. (str "Invalid binding for condition: " result-binding))))

    ;; If it's an s-expression, simply let it expand itself, and assoc the binding with the result.
    (if (#{'from :from} (second condition)) ; If this is an accumulator....
      `(map->Accumulator 
        {:result-binding ~result-binding
         :definition ~(first condition)
         :input-condition ~(construct-condition (nth condition 2) nil)})
      
      ;; Not an accumulator, so simply create the condition.
      (construct-condition condition result-binding))))

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


(declare add-rule*)

(defn insert-accumulator [accumulator more production-node alpha-roots ancestor-binding-keys]

  (let [condition (:input-condition accumulator)
        join-binding-keys (s/intersection ancestor-binding-keys (:binding-keys condition))
        all-binding-keys (s/union ancestor-binding-keys (:binding-keys condition))
        [child new-alphas] (add-rule* more production-node alpha-roots all-binding-keys)
        join-node (->AccumulateNode accumulator (:definition accumulator) [child] join-binding-keys)
        alpha-node (->AlphaNode condition 
                                [join-node] 
                                (:activate-fn condition))]
    
    [join-node, 
     (merge-with concat new-alphas {(get-in alpha-node [:condition :type]) [alpha-node]})]))

(defn- add-rule* 
  "Adds a new production, returning a tuple of a new beta root and a new set of alpha roots"
  [[expression & more :as expressions] production-node alpha-roots ancestor-binding-keys]

  ;; TODO: start by recursively adding children, then executing node-specific logic.
  ;; This redundancy needs to be cleaned up.

  (condp = (:type expression)
    
    ;; Recursively create children, then create a new join and alpha node for the condition.
    :condition 
    (if (= "clara.rules.engine.Accumulator" (.getName (class (:content expression)))) ; FIXME: check class?
      (insert-accumulator (:content expression) more production-node alpha-roots ancestor-binding-keys)
      (let [condition (:content expression)
            join-binding-keys (s/intersection ancestor-binding-keys (:binding-keys condition))
            all-binding-keys (s/union ancestor-binding-keys (:binding-keys condition))
            [child new-alphas] (add-rule* more production-node alpha-roots all-binding-keys)
            join-node  (->JoinNode condition [child] join-binding-keys)
            alpha-node (->AlphaNode condition 
                                    [join-node] 
                                    (:activate-fn condition))]
        
        [join-node, 
         (merge-with concat new-alphas {(get-in alpha-node [:condition :type]) [alpha-node]})]))
    
    ;; It's a conjunction, so add all content of the conjunction.
    :and 
    (add-rule* (:content expression) production-node alpha-roots ancestor-binding-keys)

    :not 
    (let [condition (:content (first (:content expression))) ; Get the child content of the not clause.
          join-binding-keys (s/intersection ancestor-binding-keys (:binding-keys condition))
          all-binding-keys (s/union ancestor-binding-keys (:binding-keys condition))
          [child new-alphas] (add-rule* more production-node alpha-roots all-binding-keys)
          join-node  (->NegationNode condition [child] join-binding-keys)
          alpha-node (->AlphaNode condition 
                                  [join-node] 
                                  (:activate-fn condition))]
      
      [join-node, 
       (merge-with concat new-alphas {(get-in alpha-node [:condition :type]) [alpha-node]})])

    ;; No more conditions, so terminate the recursion by returning the production node.
    ;; The returned production node will be the child of a join node built as we
    ;; work our way back up the recursion stack.
    nil [production-node alpha-roots]))

(defn- node-id [node]
  (->> node
       (tree-seq 
        #(or (sequential? %) (map? %) (set? %))
        (fn [item]
          (if (map? item)
            (concat (keys item) (vals item))
            item)))
       (filter #(or (keyword? %) (symbol? %) (number? %)))
       (hash)))

(defn- node-id-map 
  "Generates a map of unique ids to nodes."
  [beta-roots]
  (let [beta-nodes (distinct
                    (mapcat 
                     #(tree-seq :children :children %)
                     beta-roots))]
    (into {} (for [beta-node beta-nodes] [(node-id beta-node) beta-node]))))

(defn add-production* 
  "Adds a new production to the rulebase."
  [rulebase production production-node]

  (let [dnf (ast-to-dnf (:lhs production))
        ;; Get a list of disjunctions, which may be a single item.
        disjunctions (if (= :or (:type dnf)) (:content dnf) [dnf])
        [alpha-roots beta-roots] 
        (loop [alpha-roots (:alpha-roots rulebase)
               beta-roots (:beta-roots rulebase)
               disjunctions disjunctions]
          (if (seq disjunctions)
            (let [[beta-root alpha-roots] (add-rule*  [(first disjunctions)] ; FIXME: should accept simple conjunction, not a vec.
                                                      production-node
                                                      alpha-roots
                                                      #{})]
              (recur alpha-roots (conj beta-roots beta-root) (rest disjunctions)))
            [alpha-roots beta-roots]))
        id-to-node (node-id-map beta-roots)
        node-to-id (s/map-invert id-to-node)]

    (if (:rhs production)
      (->Rulebase alpha-roots 
                  beta-roots 
                  (conj (:production-nodes rulebase) production-node) 
                  (:query-nodes rulebase)
                  node-to-id
                  id-to-node)
      (->Rulebase alpha-roots 
                  beta-roots 
                  (:production-nodes rulebase) 
                  (assoc (:query-nodes rulebase) production production-node)
                  node-to-id
                  id-to-node))))

;; Active session during rule execution.
(def ^:dynamic *current-session* nil)

;; The token that triggered a rule to fire.
(def ^:dynamic *rule-context* nil)

(defrecord LocalSession [rulebase memory transport]
  ISession
  (insert [session facts]
    (let [transient-memory (to-transient memory)]
      (doseq [[cls fact-group] (group-by class facts) 
              root (get-in rulebase [:alpha-roots cls])]
        (alpha-activate root fact-group transient-memory transport))

      (->LocalSession rulebase (to-persistent! transient-memory) transport)))

  (retract [session facts]

    (let [transient-memory (to-transient memory)]
      (doseq [[cls fact-group] (group-by class facts) 
              root (get-in rulebase [:alpha-roots cls])]
        (alpha-retract root fact-group transient-memory transport))

      (->LocalSession rulebase (to-persistent! transient-memory) transport)))

  (fire-rules [session]

    (let [transient-memory (to-transient memory)]
      (binding [*current-session* {:rulebase rulebase 
                                   :transient-memory transient-memory 
                                   :transport transport
                                   :insertions (atom 0)}]
        (loop [insertion-count 0]
          (doseq [node (get-in session [:rulebase :production-nodes])
                  token (get-tokens transient-memory node {})]

            ;; Fire the node if it has not already been done for the token.
            (when (not (is-fired-token transient-memory node token))
              (binding [*rule-context* {:token token :node node}]
                ((:rhs node) session token)
                
                ;; The rule fired for the given token, so mark it as such.
                (mark-as-fired! transient-memory node token))))
          
          ;; If the rules inserted new facts, re-fire to ensure they are accounted for.
          (when (> (deref (:insertions *current-session*)) insertion-count)
            (recur (deref (:insertions *current-session*)) ))))

      (->LocalSession rulebase (to-persistent! transient-memory) transport)))

  ;; TODO: queries shouldn't require the use of transient memory.
  (query [session query params]
    (let [query-node (get-in rulebase [:query-nodes query])]
      (map :bindings (get-tokens (to-transient (:memory session)) query-node params)))))


(defn local-memory 
  "Returns a local, in-process working memory."
  [rulebase transport]     
  (let [memory (to-transient (->PersistentLocalMemory rulebase {} {} {} {}))]
    (doseq [beta-node (:beta-roots rulebase)]
      (left-activate beta-node {} [empty-token] memory transport))
    (to-persistent! memory)))

