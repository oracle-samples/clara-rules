(ns clara.rules.engine
  "The Clara rules engine. Most users should use only the clara.rules namespace."
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [clojure.set :as s]
            [clojure.string :as string]
            [clara.rules.memory :as mem]
            [clara.rules.platform :as platform]
            [clara.rules.schema :as schema]
            [schema.core :as sc]))

;; Protocol for loading rules from some arbitrary source.
(defprotocol IRuleSource
  (load-rules [source]))

;; The accumulator is a Rete extension to run an accumulation (such as sum, average, or similar operation)
;; over a collection of values passing through the Rete network. This object defines the behavior
;; of an accumulator. See the AccumulatorNode for the actual node implementation in the network.
(defrecord Accumulator [input-condition initial-value reduce-fn combine-fn convert-return-fn])

;; A rulebase -- essentially an immutable Rete network with a collection of alpha and beta nodes and supporting structure.
(defrecord Rulebase [alpha-roots beta-roots productions queries production-nodes query-nodes id-to-node]
  IRuleSource
  (load-rules [this] this)) ; A rulebase can be viewed as a rule loader; it simply loads itself.

;; A Rete-style token, containing facts and bound variables.
(defrecord Token [facts bindings])

;; A working memory element, containing a single fact and its corresponding bound variables.
(defrecord Element [fact bindings])

;; Token with no bindings, used as the root of beta nodes.
(def empty-token (->Token [] {}))

;; Returns a new session with the additional facts inserted.
(defprotocol ISession

  ;; Inserts a fact.
  (insert [session fact])

  ;; Retracts a fact.
  (retract [session fact])

  ;; Fires pending rules and returns a new session where they are in a fired state.
  (fire-rules [session])

  ;; Runs a query agains thte session.
  (query [session query params])

  ;; Returns the working memory implementation used by the session.
  (working-memory [session]))

;; Left activation protocol for various types of beta nodes.
(defprotocol ILeftActivate
  (left-activate [node join-bindings tokens memory transport])
  (left-retract [node join-bindings tokens memory transport])
  (description [node])
  (get-join-keys [node]))

;; Right activation protocol to insert new facts, connecting alpha nodes
;; and beta nodes.
(defprotocol IRightActivate
  (right-activate [node join-bindings elements memory transport])
  (right-retract [node join-bindings elements memory transport]))

;; Specialized right activation interface for accumulator nodes,
;; where the caller has the option of pre-reducing items
;; to reduce the data sent to the node. This would be useful
;; if the caller is not in the same memory space as the accumulator node itself.
(defprotocol IAccumRightActivate
  ;; Pre-reduces elements, returning a map of bindings to reduced elements.
  (pre-reduce [node elements])

  ;; Right-activate the node with items reduced in the above pre-reduce step.
  (right-activate-reduced [node join-bindings reduced  memory transport]))

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

;; Protocol for activation of Rete alpha nodes.
(defprotocol IAlphaActivate 
  (alpha-activate [node facts memory transport])
  (alpha-retract [node facts memory transport]))


;; Active session during rule execution.
(def ^:dynamic *current-session* nil)

;; The token that triggered a rule to fire.
(def ^:dynamic *rule-context* nil)

;; Record for the production node in the Rete network.
(defrecord ProductionNode [id production rhs]
  ILeftActivate  
  (left-activate [node join-bindings tokens memory transport]
    
    ;; Fire the rule if it's not a no-loop rule, or if the rule is not
    ;; active in the current context.
    (when (or (not (get-in production [:props :no-loop]))
              (not (= production (get-in *rule-context* [:node :production]))))

      ;; Preserve tokens that fired for the rule so we
      ;; can perform retractions if they become false.
      (mem/add-tokens! memory node join-bindings tokens)

      ;; The production matched, so add the tokens to the activation list.
      (mem/add-activations! memory node tokens))) 

  (left-retract [node join-bindings tokens memory transport] 
    ;; Remove any tokens to avoid future rule execution on retracted items.
    (mem/remove-tokens! memory node join-bindings tokens)

    ;; Remove pending activations triggered by the retracted tokens.
    (mem/remove-activations! memory node tokens)

    ;; Retract any insertions that occurred due to the retracted token.
    (let [insertions (mem/remove-insertions! memory node tokens)]
      (doseq [[cls fact-group] (group-by type insertions) 
              root (get-in (mem/get-rulebase memory) [:alpha-roots cls])]
        (alpha-retract root fact-group memory transport))))

  (get-join-keys [node] [])

  (description [node] "ProductionNode"))

;; The QueryNode is a terminal node that stores the
;; state that can be queried by a rule user.
(defrecord QueryNode [id query param-keys]
  ILeftActivate  
  (left-activate [node join-bindings tokens memory transport] 
    (mem/add-tokens! memory node join-bindings tokens))

  (left-retract [node join-bindings tokens memory transport] 
    (mem/remove-tokens! memory node join-bindings tokens))

  (get-join-keys [node] param-keys)

  (description [node] (str "QueryNode -- " query)))

;; Record representing alpha nodes in the Rete network,
;; each of which evaluates a single condition and
;; propagates matches to its children.
(defrecord AlphaNode [env children activation]
  IAlphaActivate
  (alpha-activate [node facts memory transport]
    (send-elements
     transport memory children
     (for [fact facts
           :let [bindings (activation fact env)] :when bindings] ; FIXME: add env.
       (->Element fact bindings))))

  (alpha-retract [node facts memory transport]

    (retract-elements
     transport memory children
     (for [fact facts
           :let [bindings (activation fact env)] :when bindings] ; FIXME: add env.
       (->Element fact bindings)))))

;; Record for the join node, a type of beta node in the rete network. This node performs joins
;; between left and right activations, creating new tokens when joins match and sending them to 
;; its descendents.
(defrecord JoinNode [id condition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport] 
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)
    (send-tokens 
     transport 
     memory
     children
     (for [element (mem/get-elements memory node join-bindings)
           token tokens
           :let [fact (:fact element)
                 fact-binding (:bindings element)]]
       (->Token (conj (:facts token) fact) (conj fact-binding (:bindings token))))))

  (left-retract [node join-bindings tokens memory transport] 
    (retract-tokens 
     transport 
     memory
     children 
     (for [token (mem/remove-tokens! memory node join-bindings tokens)
           element (mem/get-elements memory node join-bindings)
           :let [fact (:fact element)
                 fact-bindings (:bindings element)]]
       (->Token (conj (:facts token) fact) (conj fact-bindings (:bindings token))))))

  (get-join-keys [node] binding-keys)

  (description [node] (str "JoinNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport]         
    (mem/add-elements! memory node join-bindings elements)
    (send-tokens 
     transport 
     memory
     children
     (for [token (mem/get-tokens memory node join-bindings)
           {:keys [fact bindings] :as element} elements]
       (->Token (conj (:facts token) fact) (conj (:bindings token) bindings)))))

  (right-retract [node join-bindings elements memory transport]   
    (retract-tokens
     transport
     memory
     children
     (for [{:keys [fact bindings] :as element} (mem/remove-elements! memory node join-bindings elements)
           token (mem/get-tokens memory node join-bindings)]
       (->Token (conj (:facts token) fact) (conj (:bindings token) bindings))))))

;; The NegationNode is a beta node in the Rete network that simply
;; negates the incoming tokens from its ancestors. It sends tokens
;; to its descendent only if the negated condition or join fails (is false).
(defrecord NegationNode [id condition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)
    (when (empty? (mem/get-elements memory node join-bindings))
      (send-tokens transport memory children tokens)))

  (left-retract [node join-bindings tokens memory transport]
    (when (empty? (mem/get-elements memory node join-bindings))
      (retract-tokens transport memory children tokens)))

  (get-join-keys [node] binding-keys)

  (description [node] (str "NegationNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport]
    (mem/add-elements! memory node join-bindings elements)
    ;; Retract tokens that matched the activation, since they are no longer negatd.
    (retract-tokens transport memory children (mem/get-tokens memory node join-bindings)))

  (right-retract [node join-bindings elements memory transport]   
    (mem/remove-elements! memory node elements join-bindings) ;; FIXME: elements must be zero to retract.
    (send-tokens transport memory children (mem/get-tokens memory node join-bindings))))

;; The test node represents a Rete extension in which 
(defrecord TestNode [id test children]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport] 
    (send-tokens 
     transport 
     memory
     children
     (filter test tokens)))

  (left-retract [node join-bindings tokens memory transport] 
    (retract-tokens transport  memory children tokens))

  (get-join-keys [node] [])

  (description [node] (str "TestNode -- " (:text test))))

(defn- retract-accumulated 
  "Helper function to retract an accumulated value."
  [node accumulator result-binding token result fact-bindings transport memory]
  (let [converted-result ((:convert-return-fn accumulator) result)
        new-facts (conj (:facts token) converted-result)
        new-bindings (merge (:bindings token) 
                            fact-bindings
                            (when result-binding
                              { result-binding
                                converted-result}))] 

    (retract-tokens transport memory (:children node) 
                    [(->Token new-facts new-bindings)])))

(defn- send-accumulated 
  "Helper function to send the result of an accumulated value to the node's children."
  [node accumulator result-binding token result fact-bindings transport memory]
  (let [converted-result ((:convert-return-fn accumulator) result)
        new-bindings (merge (:bindings token) 
                            fact-bindings
                            (when result-binding
                              { result-binding
                                converted-result}))]

    (send-tokens transport memory (:children node)
                 [(->Token (conj (:facts token) converted-result) new-bindings)])))

(defn- has-keys? 
  "Returns true if the given map has all of the given keys."
  [m keys]
  (every? (partial contains? m) keys))

;; The AccumulateNode hosts Accumulators, a Rete extension described above, in the Rete network
;; It behavios similarly to a JoinNode, but performs an accumulation function on the incoming
;; working-memory elements before sending a new token to its descendents.
(sc/defrecord AccumulateNode [id accumulator result-binding children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport] 
    (let [previous-results (mem/get-accum-reduced-all memory node join-bindings)]
      (mem/add-tokens! memory node join-bindings tokens)

      (doseq [token tokens]

        (cond

         ;; If there are previously accumulated results to propagate, simply use them.
         (seq previous-results)
         (doseq [[fact-bindings previous] previous-results]
           (send-accumulated node accumulator result-binding token previous fact-bindings transport memory))

         ;; There are no previously accumulated results, but we still may need to propagate things
         ;; such as a sum of zero items.
         ;; If all variables in the accumulated item are bound and an initial
         ;; value is provided, we can propagate the initial value as the accumulated item.

         (and (has-keys? (:bindings token) 
                         binding-keys) ; All bindings are in place.
              (:initial-value accumulator)) ; An initial value exists that we can propagate.
         (let [fact-bindings (select-keys (:bindings token) binding-keys)
               previous (:initial-value accumulator)]

           ;; Send the created accumulated item to the children.
           (send-accumulated node accumulator result-binding token previous fact-bindings transport memory)

           ;; Add it to the working memory.
           (mem/add-accum-reduced! memory node join-bindings previous fact-bindings))
         
         ;; Propagate nothing if the above conditions don't apply.
         :default nil))))

  (left-retract [node join-bindings tokens memory transport] 
    (let [previous-results (mem/get-accum-reduced-all memory node join-bindings)]
      (doseq [token (mem/remove-tokens! memory node join-bindings tokens)
              [fact-bindings previous] previous-results]
        (retract-accumulated node accumulator result-binding token previous fact-bindings transport memory))))

  (get-join-keys [node] binding-keys)

  (description [node] (str "AccumulateNode -- " accumulator))

  IAccumRightActivate
  (pre-reduce [node elements]    
    ;; Return a map of bindings to the pre-reduced value.
    (for [[bindings element-group] (group-by :bindings elements)]      
      [bindings 
       (r/reduce (:reduce-fn accumulator) 
                 (:initial-value accumulator) 
                 (r/map :fact element-group))])) 
  
  (right-activate-reduced [node join-bindings reduced-seq  memory transport]
    ;; Combine previously reduced items together, join to matching tokens,
    ;; and emit child tokens.
    (doseq [:let [matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings reduced] reduced-seq
            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)]]

      ;; If the accumulation result was previously calculated, retract it
      ;; from the children.
      (when previous
        
        (doseq [token (mem/get-tokens memory node join-bindings)]
          (retract-accumulated node accumulator result-binding token previous bindings transport memory)))

      ;; Combine the newly reduced values with any previous items.
      (let [combined (if previous
                       ((:combine-fn accumulator) previous reduced)
                       reduced)]        

        (mem/add-accum-reduced! memory node join-bindings combined bindings)
        (doseq [token matched-tokens]          
          (send-accumulated node accumulator result-binding token combined bindings transport memory)))))

  IRightActivate
  (right-activate [node join-bindings elements memory transport]

    ;; Simple right-activate implementation simple defers to
    ;; accumulator-specific logic.
    (right-activate-reduced
     node 
     join-bindings
     (pre-reduce node elements)
     memory 
     transport))

  (right-retract [node join-bindings elements memory transport]   

    (doseq [:let [matched-tokens (mem/get-tokens memory node join-bindings)]
            {:keys [fact bindings] :as element} elements
            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)]
            
            ;; No need to retract anything if there was no previous item.
            :when previous

            ;; Get all of the previously matched tokens so we can retract and re-send them.
            token matched-tokens

            ;; Compute the new version with the retracted information.
            :let [retracted ((:retract-fn accumulator) previous fact)]]

      ;; Add our newly retracted information to our node.
      (mem/add-accum-reduced! memory node join-bindings retracted bindings)

      ;; Retract the previous token.
      (retract-accumulated node accumulator result-binding token previous bindings transport memory)

      ;; Send a new accumulated token with our new, retracted information.
      (when retracted
        (send-accumulated node accumulator result-binding token retracted bindings transport memory)))))


(defn variables-as-keywords
  "Returns symbols in the given s-expression that start with '?' as keywords"
  [expression]
  (into #{} (for [item (flatten expression) 
                  :when (and (symbol? item) 
                             (= \? (first (name item))))] 
              (keyword item))))

(defn conj-rulebases 
  "DEPRECATED. Simply concat sequences of rules and queries.

   Conjoin two rulebases, returning a new one with the same rules."
  [base1 base2]
  (concat base1 base2))

(defn fire-rules* 
  "Fire rules for the given nodes."
  [rulebase nodes transient-memory transport get-alphas-fn]
  (binding [*current-session* {:rulebase rulebase 
                               :transient-memory transient-memory 
                               :transport transport
                               :insertions (atom 0)
                               :get-alphas-fn get-alphas-fn}]
    
    (loop [activations (mem/get-activations transient-memory)]

      ;; Clear the activations we're processing; new ones may
      ;; be added during insertions.         
      (mem/clear-activations! transient-memory)

      (doseq [[node tokens] activations
              token tokens]
        (binding [*rule-context* {:token token :node node}]
          ((:rhs node) token (:env (:production node)))))
      
      ;; If new activations were created, loop to fire those as well.
      (when (seq (mem/get-activations transient-memory))
        (recur (mem/get-activations transient-memory))))))

(defn create-get-alphas-fn
  "Returns a function that given a sequence of facts,
  returns a map associating alpha nodes with the facts they accept."
  [fact-type-fn merged-rules]
  
  ;; We preserve a map of fact types to alpha nodes for efficiency,
  ;; effectively memoizing this operation.
  (let [alpha-map (atom {})]    
    (fn [facts]
      (for [[fact-type facts] (group-by fact-type-fn facts)]
        
        (if-let [alpha-nodes (get @alpha-map fact-type)]
          
          ;; If the matching alpha nodes are cached, simply return them.
          [alpha-nodes facts]
          
          ;; The alpha nodes weren't cached for the type, so get them now.
          (let [ancestors (conj (ancestors fact-type) fact-type)
                
                ;; Get all alpha nodes for all ancestors.
                new-nodes (distinct 
                           (reduce 
                            (fn [coll ancestor] 
                              (concat 
                               coll 
                               (get-in merged-rules [:alpha-roots ancestor])))
                            []
                            ancestors))]
            
            (swap! alpha-map assoc fact-type new-nodes)
            [new-nodes facts]))))))

(deftype LocalSession [rulebase memory transport get-alphas-fn]
  ISession
  (insert [session facts]
    (let [transient-memory (mem/to-transient memory)]
      (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
              root alpha-roots]
        (alpha-activate root fact-group transient-memory transport))
      (LocalSession. rulebase (mem/to-persistent! transient-memory) transport get-alphas-fn)))

  (retract [session facts]

    (let [transient-memory (mem/to-transient memory)]
      (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
              root alpha-roots]
        (alpha-retract root fact-group transient-memory transport))

      (LocalSession. rulebase (mem/to-persistent! transient-memory) transport get-alphas-fn)))

  (fire-rules [session]

    (let [transient-memory (mem/to-transient memory)]
      (fire-rules* rulebase 
                   (:production-nodes rulebase)
                   transient-memory
                   transport
                   get-alphas-fn)

      (LocalSession. rulebase (mem/to-persistent! transient-memory) transport get-alphas-fn)))

  ;; TODO: queries shouldn't require the use of transient memory.
  (query [session query params]
    (let [query-node (get-in rulebase [:query-nodes query])]
      (when (= nil query-node) 
        (platform/throw-error "The given query is invalid or not included in the rule base."))
      (map :bindings (mem/get-tokens (mem/to-transient (working-memory session)) query-node params))))
  
  (working-memory [session] memory))


(defn local-memory 
  "Returns a local, in-process working memory."
  [rulebase transport]     
  (let [memory (mem/to-transient (mem/->PersistentLocalMemory rulebase {}))]
    (doseq [beta-node (:beta-roots rulebase)]
      (left-activate beta-node {} [empty-token] memory transport))
    (mem/to-persistent! memory)))


(defn- expr-type [expression]
  (if (map? expression)
    :condition
    (first expression)))

(defn- cartesian-join [lists lst]
  (if (seq lists)
    (let [[h & t] lists]
      (mapcat 
       (fn [l]
         (map #(conj % l) (cartesian-join t lst)))
       h))
    [lst]))

(defn to-dnf 
  "Convert a lhs expression to disjunctive normal form."
  [expression] 
  
  ;; Always validate the expression schema, as this is only done at compile time.
  (sc/validate schema/Condition expression)
  (condp = (expr-type expression)
    ;; Individual conditions can return unchanged.
    :condition
    expression

    :test
    expression

    ;; Apply de Morgan's law to push negation nodes to the leaves. 
    :not
    (let [children (rest expression)
          child (first children)]

      (when (not= 1 (count children))
        (throw (RuntimeException. "Negation must have only one child.")))

      (condp = (expr-type child)

        ;; If the child is a single condition, simply return the ast.
        :condition expression

        :test expression

        ;; DeMorgan's law converting conjunction to negated disjuctions.
        :and (to-dnf (into [:or] (for [grandchild (rest child)] [:not grandchild])))
        
        ;; DeMorgan's law converting disjuction to negated conjuctions.
        :or  (to-dnf (into [:and] (for [grandchild (rest child)] [:not grandchild])))))
    
    ;; For all others, recursively process the children.
    (let [children (map to-dnf (rest expression))
          ;; Get all conjunctions, which will not conain any disjunctions since they were processed above.
          conjunctions (filter #(#{:and :condition :not} (expr-type %)) children)]
      
      ;; If there is only one child, the and or or operator can simply be eliminated.
      (if (= 1 (count children))
        (first children)

        (condp = (expr-type expression)

          :and
          (let [disjunctions (map rest (filter #(= :or (expr-type %)) children))]
            (if (empty? disjunctions)
              (into [:and] (apply concat 
                                  (for [child children]
                                    (if (= :and (expr-type child))
                                      (rest child)
                                      [child]))))
              (into [:or] 
                    (for [c (cartesian-join disjunctions conjunctions)] 
                      (into [:and] c)))))
          :or
          ;; Merge all child disjunctions into a single list.
          (let [disjunctions (mapcat rest (filter #(#{:or} (expr-type %)) children))]
            (into [:or] (concat disjunctions conjunctions))))))))



(defn- add-to-beta-tree
  "Adds a sequence of conditions and the corresponding production to the beta tree."
  [beta-nodes
   [[condition env] & more] 
   bindings 
   production]
  (let [is-negation (= :not (first condition))
        accumulator (:accumulator condition)
        result-binding (:result-binding condition) ; Get the optional result binding used by accumulators.
        condition (cond
                   is-negation (second condition) 
                   accumulator (:from condition)
                   :default condition)        
        node-type (cond
                   is-negation :negation
                   accumulator :accumulator
                   (:type condition) :join
                   :else :test)

        ;; For the sibling beta nodes, find a match for the candidate.
        matching-node (first (for [beta-node beta-nodes
                                   :when (and (= condition (:condition beta-node))
                                              (= node-type (:node-type beta-node))
                                              (= env (:env beta-node))
                                              (= accumulator (:accumulator beta-node)))]
                               beta-node))

        other-nodes (remove #(= matching-node %) beta-nodes)
        cond-bindings (variables-as-keywords (:constraints condition))

        ;; Create either the rule or query node, as appropriate.
        production-node (if (:rhs production) 
                          {:node-type :production
                           :production production}
                          {:node-type :query
                           :query production})]

    (vec
     (conj
      other-nodes
      (if condition
        ;; There are more conditions, so recurse.
        (if matching-node
          (assoc matching-node 
            :children 
            (add-to-beta-tree (:children matching-node) more (s/union bindings cond-bindings) production))

          (cond-> 
           {:node-type node-type
            :condition condition
            :children (add-to-beta-tree [] more (s/union bindings cond-bindings) production)}

           ;; Add the join bindings to join, accumulator or negation nodes.
           (#{:join :negation :accumulator} node-type) (assoc :join-bindings (s/intersection bindings cond-bindings))

           accumulator (assoc :accumulator accumulator)
           
           result-binding (assoc :result-binding result-binding)

           env (assoc :env env)))

        ;; There are no more conditions, so add our query or rule.
        (if matching-node
          (update-in matching-node [:children] conj production-node)
          production-node))))))

(defn- condition-comp 
  "Helper function to sort conditions to ensure bindings
   are created in the needed order. The current implementation
   simply pushes tests to the end (since they don't create new bindings)
   with accumulators before them, as they may rely on previously bound items
   to complete successfully."
  [cond1 cond2]

  (letfn [(cond-type [condition]
            (cond 
             (:type condition) :condition
             (:accumulator condition) :accumulator
             (= :not (first condition)) :negation
             :default :test))]

    (case (cond-type cond1)
      ;; Conditions are always sorted first.
      :condition true

      ;; Negated conditions occur before tests and accumulators.
      :negation (boolean (#{:test :accumulator} (cond-type cond2)))

      ;; Accumulators are sorted before tests.
      :accumulator (= :test (cond-type cond2))

      ;; Tests are last.
      :test false)))

(sc/defn to-beta-tree :- [schema/BetaNode]
  "Convert a sequence of rules and/or queries into a beta tree. Returns each root."
  [productions :- [schema/Production]]
  (let [conditions (for [production productions
                         :let [lhs-expression (into [:and] (:lhs production)) ; Add implied and.
                               expression  (to-dnf lhs-expression)]
                         disjunction (if (= :or (first expression))
                                       (rest expression)
                                       [expression])
                         :let [conditions (if (and (vector? disjunction) 
                                                   (= :and (first disjunction)))
                                            (rest disjunction)
                                            [disjunction])
                               
                               ;; Sort conditions, see the condition-comp function for the reason.
                               sorted-conditions (sort condition-comp conditions) 

                               ;; Attach the conditions environment. TODO: narrow environment to those used?
                               conditions-with-env (for [condition sorted-conditions]
                                                     [condition (:env production)])]]

                     [conditions-with-env production])

        raw-roots (reduce 
                   (fn [beta-roots [conditions production]]
                    (add-to-beta-tree beta-roots conditions #{} production))
                   []
                   conditions)

        nodes (for [root raw-roots
                    node (tree-seq :children :children root)]
                node)

        ;; Sort nodes so the same id is assigned consistently,
        ;; then map the to corresponding ids.
        nodes-to-id (zipmap 
                     (sort #(< (hash %1) (hash %2)) nodes)
                     (range))
        
        ;; Anonymous function to walk the nodes and
        ;; assign identifiers to them.
        assign-ids-fn (fn assign-ids [node]
                        (if (:children node)
                          (merge node
                                 {:id (nodes-to-id node)
                                  :children (map assign-ids (:children node))})
                          (assoc node :id (nodes-to-id node))))]
    
    ;; Assign IDs to the roots and return them.
    (map assign-ids-fn raw-roots)))

(sc/defn to-alpha-tree :- [schema/AlphaNode]
  "Returns a sequence of [condition-fn, [node-ids]] tuples to represent the alpha side of the network."
  [beta-roots :- [schema/BetaNode]]

  ;; Create a sequence of tuples of conditions + env to beta node ids.
  (let [condition-to-node-ids (for [root beta-roots
                                    node (tree-seq :children :children root)
                                    :when (:condition node)]
                                [[(:condition node) (:env node)] (:id node)])

        ;; Merge common conditions together.
        condition-to-node-map (reduce
                               (fn [node-map [[condition env] node-id]]

                                 ;; Can't use simple update-in because we need to ensure
                                 ;; the value is a vector, not a list.
                                 (if (get node-map [condition env])
                                   (update-in node-map [[condition env]] conj node-id)
                                   (assoc node-map [condition env] [node-id])))
                               {}
                               condition-to-node-ids)]

    ;; Compile conditions into functions.
    (vec
     (for [[[condition env] node-ids] condition-to-node-map
           :when (:type condition) ; Exclude test conditions.
           ]

       (cond-> {:condition condition
                :beta-children node-ids}
               env (assoc :env env))))))

;; FIXME: reuse logic in to-alpha-tree....
(sc/defn to-alpha-nodes :- [{:type sc/Any 
                             :alpha-fn sc/Any ;; TODO: is a function...
                             (sc/optional-key :env) {sc/Keyword sc/Any}
                             :children [sc/Number]}]
  "Returns a sequence of [condition-fn, [node-ids]] tuples to represent the alpha side of the network."
  [beta-roots :- [schema/BetaNode]
   {alpha-compile-fn :alpha-compile-fn}]

  ;; Create a sequence of tuples of conditions + env to beta node ids.
  (let [condition-to-node-ids (for [root beta-roots
                                    node (tree-seq :children :children root)
                                    :when (:condition node)]
                                [[(:condition node) (:env node)] (:id node)])

        ;; Merge common conditions together.
        condition-to-node-map (reduce
                               (fn [node-map [[condition env] node-id]]

                                 ;; Can't use simple update-in because we need to ensure
                                 ;; the value is a vector, not a list.
                                 (if (get node-map [condition env])
                                   (update-in node-map [[condition env]] conj node-id)
                                   (assoc node-map [condition env] [node-id])))
                               {}
                               condition-to-node-ids)]

    ;; Compile conditions into functions.
    (vec
     (for [[[condition env] node-ids] condition-to-node-map
           :when (:type condition) ; Exclude test conditions.
           :let [{:keys [type constraints fact-binding args]} condition]]

       (cond-> {:type type
                :alpha-fn (alpha-compile-fn type (first args) constraints fact-binding env)
                :children node-ids}
               env (assoc :env env))))))

(sc/defn compile-beta-tree
  "Compile the beta tree to the nodes used at runtime."
  ([beta-nodes  :- [schema/BetaNode]
    parent-bindings
    system-env]
     (vec
      (for [beta-node beta-nodes
            :let [{:keys [condition children id production query join-bindings]} beta-node

                  constraint-bindings (variables-as-keywords (:constraints condition))

                  ;; Get all bindings from the parent, condition, and returned fact.
                  all-bindings (cond-> (s/union parent-bindings constraint-bindings)
                                       (:fact-binding condition) (conj (:fact-binding condition)))]] 

        (case (:node-type beta-node)

          :join
          (->JoinNode 
           id
           condition  
           (compile-beta-tree children all-bindings system-env)
           join-bindings)

          :negation
          (->NegationNode
           id
           condition  
           (compile-beta-tree children all-bindings system-env)
           join-bindings)

          :test          
          (->TestNode 
           id
           ((:test-compile-fn system-env) (:constraints condition))  
           (compile-beta-tree children all-bindings system-env))

          :accumulator
          (->AccumulateNode
           id 
           (((:accum-compile-fn system-env) (:accumulator beta-node) (:env beta-node)) (:env beta-node))
           (:result-binding beta-node)
           (compile-beta-tree children all-bindings system-env)
           join-bindings)

          :production
          (->ProductionNode 
           id
           production 
           ((:rhs-compile-fn system-env) all-bindings production))
         
          :query
          (->QueryNode
           id
           query
           (:params query))
          )))))


(sc/defn build-network
  "Constructs the network from compiled beta tree and condition functions."
  [beta-roots alpha-fns productions]
  
  (let [beta-nodes (for [root beta-roots
                         node (tree-seq :children :children root)]
                     node)

        production-nodes (for [node beta-nodes
                               :when (= ProductionNode (type node))]
                           node)

        query-nodes (for [node beta-nodes
                          :when (= QueryNode (type node))]
                      node)

        ;; TOOD: assign query names as map keys as well?
        query-map (into {} (for [query-node query-nodes]
                             [(:query query-node) query-node]))

        ;; Map of node ids to beta nodes.
        id-to-node (into {} (for [node beta-nodes]
                                 [(:id node) node]))
        
        ;; type, alpha node tuples.
        alpha-nodes (for [{:keys [type alpha-fn children env]} alpha-fns
                          :let [beta-children (map id-to-node children)]]
                      [type (->AlphaNode env beta-children alpha-fn)])

        ;; Merge the alpha nodes into a multi-map
        alpha-map (reduce
                   (fn [alpha-map [type alpha-node]]
                     (update-in alpha-map [type] conj alpha-node))
                   {}
                   alpha-nodes)]
    
    (map->Rulebase 
     {:alpha-roots alpha-map
      :beta-roots beta-roots
      :productions (filter :rhs productions)
      :queries (remove :rhs productions)
      :production-nodes production-nodes
      :query-nodes query-map
      :id-to-node id-to-node})))


;; Cache of sessions for fast reloading.
(def ^:private session-cache (atom {}))

(sc/defn mk-session* 
  "Compile the rules into a rete network and return the given session."
  [productions :- [schema/Production]
   options :- {sc/Keyword sc/Any}
   system-env :- {:alpha-compile-fn sc/Any
                  :rhs-compile-fn sc/Any
                  :test-compile-fn sc/Any
                  :accum-compile-fn sc/Any}]
  (let [beta-struct (to-beta-tree productions)
        beta-tree (compile-beta-tree beta-struct #{} system-env)
        alpha-nodes (to-alpha-nodes beta-struct system-env)
        rulebase (build-network beta-tree alpha-nodes productions)
        transport (LocalTransport.)

        ;; The fact-type uses Clojure's type function unless overridden.
        fact-type-fn (get options :fact-type-fn type)

        ;; Create a function that groups a sequence of facts by the collection
        ;; of alpha nodes they target.
        ;; We cache an alpha-map for facts of a given type to avoid computing
        ;; them for every fact entered.
        get-alphas-fn (create-get-alphas-fn fact-type-fn rulebase)]

    (LocalSession. rulebase (local-memory rulebase transport) transport get-alphas-fn)))

(defn mk-session
  "Creates a new session using the given rule source. Thew resulting session
   is immutable, and can be used with insert, retract, fire-rules, and query functions."
  ([sources-and-options
    system-env]

     ;; If an equivalent session has been created, simply reuse it.
     ;; This essentially memoizes this function unless the caller disables caching.
     (if-let [session (get @session-cache [sources-and-options])]
       session

       ;; Separate sources and options, then load them.
       (let [sources (take-while (complement keyword?) sources-and-options)
             options (apply hash-map (drop-while (complement keyword?) sources-and-options))
             productions (mapcat
                          #(if (satisfies? IRuleSource %)
                             (load-rules %)
                             %)
                          sources) ; Load rules from the source, or just use the input as a seq.
             session (mk-session* productions options system-env)]

         ;; Cache the session unless instructed not to.
         (when (get options :cache true)
           (swap! session-cache assoc [sources-and-options] session))
         
         ;; Return the session.
         session))))