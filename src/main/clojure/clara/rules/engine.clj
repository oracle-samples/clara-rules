(ns clara.rules.engine
  "The Clara rules engine. Most users should use only the clara.rules namespace."
  (:use clara.rules.memory
        clojure.pprint)
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [clojure.set :as s])
  (:refer-clojure :exclude [==]))

;; Protocol for loading rules from some arbitrary source.
(defprotocol IRuleSource
  (load-rules [source]))


(defprotocol ICondition
  ;; Returns a key uniquely identifying the condition.
  (condition-key [condition])
  ;; Returns the type of the matching object.
  (match-type [condition])
  ;; Returns the keys that may be bound by the condition.
  (binding-keys [condition])
  ;; Returns the mode of the condition, one of :normal, :accumulator, :test, or :negation
  (mode [condition])
  ;; Returns a function that activates the condition.
  (activate-fn [condition])  
  ;; Returns the base condition for use on the alpha side of the network,
  ;; e.g. without negation or accumulation.
  (alpha-condition [condition]))

;; Record defining a single condition in a rule production.
(defrecord Condition [type constraints binding-keys activate-fn text]

  ICondition
  (condition-key [condition] {:type type :constraints constraints})
  (match-type [condition] type)
  (binding-keys [condition] binding-keys)
  (mode [condition] :normal)
  (activate-fn [condition] activate-fn)
  (alpha-condition [condition] condition))

;; The accumulator is a Rete extension to run an accumulation (such as sum, average, or similar operation)
;; over a collection of values passing through the Rete network. This object defines the behavior
;; of an accumulator. See the AccumulatorNode for the actual node implementation in the network.
(defrecord Accumulator [result-binding input-condition initial-value reduce-fn combine-fn convert-return-fn]
  ICondition
  ;; Key uses the accumulator itself to ensure uniqueness. Is there a better way to handle this?
  (condition-key [condition] (assoc (condition-key input-condition) :accum condition)) 
  (match-type [condition] (:type input-condition))
  (binding-keys [condition] (:binding-keys input-condition))
  (mode [condition] :accumulator)
  (activate-fn [condition] (activate-fn input-condition))
  (alpha-condition [condition] input-condition))

;; A negated condition.
(defrecord NegatedCondition [input-condition]
  ICondition
  (condition-key [condition] (condition-key input-condition))
  (match-type [condition] (:type input-condition))
  (binding-keys [condition] (:binding-keys input-condition))
  (mode [condition] :negation)
  (activate-fn [condition] (activate-fn input-condition))
  (alpha-condition [condition] input-condition))

;; Record containing one or more test expressions to evaluate.
(defrecord Test [test-fn constraints text]
  ICondition
  (condition-key [condition] {:type :none :constraints constraints})
  (match-type [condition] :none)
  (binding-keys [condition] #{})
  (mode [condition] :test)
  (activate-fn [condition] test-fn)
  (alpha-condition [condition] nil))

;; Record defining a production, where lhs is the left-hand side containing constraints, and rhs is right-hand side.
(defrecord Production [lhs rhs])

;; Record defining a query given the parameters and expected bindings used in the query.
(defrecord Query [params lhs binding-keys])

;; A rulebase -- essentially an immutable Rete network with a collection of alpha and beta nodes and supporting structure.
(defrecord Rulebase [alpha-roots beta-roots productions queries production-nodes query-nodes node-to-id id-to-node]
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

;; Right 
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

;; Record for the production node in the Rete network.
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

  (description [node] "ProductionNode"))

(defrecord QueryNode [query param-keys]
  ILeftActivate  
  (left-activate [node join-bindings tokens memory transport] 
    (add-tokens! memory node join-bindings tokens))

  (left-retract [node join-bindings tokens memory transport] 
    (remove-tokens! memory node join-bindings tokens))

  (get-join-keys [node] param-keys)

  (description [node] (str "QueryNode -- " param-keys)))

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

;; Record for the join node, a type of beta node in the rete network. This node performs joins
;; between left and right activations, creating new tokens when joins match and sending them to 
;; its descendents.
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

  (description [node] (str "JoinNode -- " (:text condition)))

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
     (for [{:keys [fact bindings] :as element} (remove-elements! memory node join-bindings elements)
           token (get-tokens memory node join-bindings)]
       (->Token (conj (:facts token) fact) (conj (:bindings token) bindings))))))

;; The NegationNode is a beta node in the Rete network that simply
;; negates the incoming tokens from its ancestors. It sends tokens
;; to its descendent only if the negated condition or join fails (is false).
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

  (description [node] (str "NegationNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport]
    (add-elements! memory node join-bindings elements)
    ;; Retract tokens that matched the activation, since they are no longer negatd.
    (retract-tokens transport memory children (get-tokens memory node join-bindings)))

  (right-retract [node join-bindings elements memory transport]   
    (remove-elements! memory node elements join-bindings) ;; FIXME: elements must be zero to retract.
    (send-tokens transport memory children (get-tokens memory node join-bindings))))

;; The test node represents a Rete extension in which 
(defrecord TestNode [test children]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport] 
    (send-tokens 
     transport 
     memory
     children
     (filter #((:test-fn test) %) tokens)))

  (left-retract [node join-bindings tokens memory transport] 
    (retract-tokens transport  memory children tokens))

  (get-join-keys [node] [])

  (description [node] (str "TestNode -- " (:text test))))

(defn- retract-accumulated 
  "Helper function to retract an accumulated value."
  [node accumulator token result fact-bindings transport memory]
  (let [converted-result ((get-in accumulator [:definition :convert-return-fn]) result)
        new-facts (conj (:facts token) converted-result)
        new-bindings (merge (:bindings token) 
                            fact-bindings
                            (when (:result-binding accumulator) 
                              { (:result-binding accumulator) 
                                converted-result}))] 

    (retract-tokens transport memory (:children node) 
                    [(->Token new-facts new-bindings)])))

(defn- send-accumulated 
  "Helper function to send the result of an accumulated value to the node's children."
  [node accumulator token result fact-bindings transport memory]
  (let [converted-result ((get-in accumulator [:definition :convert-return-fn]) result)
        new-bindings (merge (:bindings token) 
                            fact-bindings
                            (when (:result-binding accumulator) 
                              { (:result-binding accumulator) 
                                converted-result}))]

    (send-tokens transport memory (:children node)
                 [(->Token (conj (:facts token) converted-result) new-bindings)])))

;; The AccumulateNode hosts Accumulators, a Rete extension described above, in the Rete network
;; It behavios similarly to a JoinNode, but performs an accumulation function on the incoming
;; working-memory elements before sending a new token to its descendents.
(defrecord AccumulateNode [accumulator definition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport] 
    (let [previous-results (get-accum-reduced-all memory node join-bindings)]
      (add-tokens! memory node join-bindings tokens)
      (doseq [token tokens
              [fact-bindings previous] previous-results]
        (send-accumulated node accumulator token previous fact-bindings transport memory))))

  (left-retract [node join-bindings tokens memory transport] 
    (let [previous-results (get-accum-reduced-all memory node join-bindings)]
      (doseq [token (remove-tokens! memory node join-bindings tokens)
              [fact-bindings previous] previous-results]
        (retract-accumulated node accumulator token previous fact-bindings transport memory))))

  (get-join-keys [node] binding-keys)

  (description [node] (str "AccumulateNode -- " (:text accumulator)))

  IAccumRightActivate
  (pre-reduce [node elements]    
    ;; Return a map of bindings to the pre-reduced value.
    (for [[bindings element-group] (group-by :bindings elements)]      
      [bindings 
       (r/reduce (:reduce-fn definition) 
                 (:initial-value definition) 
                 (r/map :fact element-group))])) 
 
  (right-activate-reduced [node join-bindings reduced-seq  memory transport]
    ;; Combine previously reduced items together, join to matching tokens,
    ;; and emit child tokens.
    (doseq [:let [matched-tokens (get-tokens memory node join-bindings)]
            [bindings reduced] reduced-seq
            :let [previous (get-accum-reduced memory node join-bindings bindings)]]

      ;; If the accumulation result was previously calculated, retract it
      ;; from the children.
      (when previous
      
        (doseq [token (get-tokens memory node join-bindings)]
          (retract-accumulated node accumulator token previous bindings transport memory)))

      ;; Combine the newly reduced values with any previous items.
      (let [combined (if previous
                       ((:combine-fn definition) previous reduced)
                       reduced)]        

        (add-accum-reduced! memory node join-bindings combined bindings)
        (doseq [token matched-tokens]          
          (send-accumulated node accumulator token combined bindings transport memory)))))

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

    (doseq [:let [matched-tokens (get-tokens memory node join-bindings)]
            {:keys [fact bindings] :as element} elements
            :let [previous (get-accum-reduced memory node join-bindings bindings)]
            
            ;; No need to retract anything if there was no previous item.
            :when previous

            ;; Get all of the previously matched tokens so we can retract and re-send them.
            token matched-tokens

            ;; Compute the new version with the retracted information.
            :let [retracted ((:retract-fn definition) previous fact)]]

      ;; Add our newly retracted information to our node.
      (add-accum-reduced! memory node join-bindings retracted bindings)

      ;; Retract the previous token.
      (retract-accumulated node accumulator token previous bindings transport memory)

      ;; Send a new accumulated token with our new, retracted information.
      (when retracted
        (send-accumulated node accumulator token retracted bindings transport memory)))))

(def ^:private reflector
  "For some reason (bug?) the default reflector doesn't use the
  Clojure dynamic class loader, which prevents reflecting on
  `defrecords`.  Work around by supplying our own which does."
  (clojure.reflect.JavaReflector. (clojure.lang.RT/baseLoader)))

(defn- get-field-accessors
  "Returns a map of field name to a symbol representing the function used to access it."
  [cls]
  (into {}
        (for [member (:members (reflect/type-reflect cls :reflector reflector))
              :when  (and (:type member) 
                          (not (#{'__extmap '__meta} (:name member)))
                          (:public (:flags member))
                          (not (:static (:flags member))))]
          [(:name member) (symbol (str ".-" (:name member)))])))

(defn- get-bean-accessors
  "Returns a map of bean property name to a symbol representing the function used to access it."
  [cls]
  (into {}
        ;; Iterate through the bean properties, returning tuples and the corresponding methods.
        (for [property (seq (.. java.beans.Introspector 
                                (getBeanInfo cls) 
                                (getPropertyDescriptors)))]

          [(symbol (.. property (getName))) 
           (symbol (str "." (.. property (getReadMethod) (getName))))])))

(defn- compile-constraints [exp-seq assigment-set]
  (if (empty? exp-seq)
    `((deref ~'?__bindings__))
    (let [ [[cmp a b :as exp] & rest] exp-seq
           compiled-rest (compile-constraints rest assigment-set)
           containEq? (and (symbol? cmp) (let [cmp-str (name cmp)] (or (= cmp-str "=") (= cmp-str "==")))) 
           a-in-assigment (and containEq? (and (symbol? a) (assigment-set (keyword a))))
           b-in-assigment (and containEq? (and (symbol? b) (assigment-set (keyword b))))]
       (cond
        a-in-assigment
        (if b-in-assigment
          (cons `(swap! ~'?__bindings__ assoc ~(keyword a) (~'?__bindings__ ~(keyword b))) compiled-rest)
          (cons `(swap! ~'?__bindings__ assoc ~(keyword a) ~b) compiled-rest))
        b-in-assigment
        (cons `(swap! ~'?__bindings__ assoc ~(keyword b) ~a) compiled-rest)
        ;; not a unification
        :else
        (list (list 'if exp (cons 'do compiled-rest) nil))))))  

(defn- compile-condition 
  "Returns a function definition that can be used in alpha nodes to test the condition."
  [type constraints binding-keys result-binding]
  (let [;; Get a map of fieldnames to access function symbols.
        accessors (if (isa? type clojure.lang.IRecord) 
                    (get-field-accessors type)
                    (get-bean-accessors type)) ; Treat unrecognized types as beans.

        ;; Convert the accessor map to an assignment block that can be used in a let expression.
        assignments (mapcat (fn [[name accessor]] 
                              [name (list accessor 'this)]) 
                            accessors)

        ;; Initial bindings used in the return of the compiled condition expresion.
        initial-bindings (if result-binding {result-binding 'this}  {})]

    `(fn [ ~(with-meta 
              'this 
              {:tag (symbol (.getName type))})] ; Add type hint to avoid runtime refection.

       (let [~@assignments
             ~'?__bindings__ (atom ~initial-bindings)]
         (do ~@(compile-constraints constraints (set binding-keys)))))))

(defn compile-action [binding-keys rhs]
  (let [assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)]
    `(fn [~'?__token__] 
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
        constraints (vec (rest condition))
        binding-keys (variables-as-keywords constraints)
        text (pr-str condition)]
       
    `(map->Condition {:type ~(resolve type) 
                      :constraints '~constraints 
                      :binding-keys ~binding-keys 
                      :activate-fn ~(compile-condition (resolve type) constraints binding-keys result-binding)
                      :text ~text})))

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
         :input-condition ~(construct-condition (nth condition 2) nil)
         :text ~(pr-str condition)})
      
      ;; Not an accumulator, so simply create the condition.
      (construct-condition condition result-binding))))

;; Let operators be symbols or keywords.
(def operators #{'and 'or 'not :and :or :not})

(defn mk-test [tests]
  (let [binding-keys (variables-as-keywords tests)
        assignments (mapcat #(list (symbol (name %)) (list 'get-in '?__token__ [:bindings %])) binding-keys)]
    `(->Test 
      (fn [~'?__token__] 
        (let [~@assignments]
          (and ~@tests)))
      '~tests
      ~(pr-str tests))))

(defn- parse-expression [expression]
  (cond 
   (operators (first expression))
   {:type (keyword (first expression)) 
    :content (vec (map parse-expression (rest expression)))}

   (#{'test :test} (first expression))
   {:type :test 
    :content (mk-test (rest expression))}

   :default
   {:type :condition :content (create-condition expression)}))

(defn parse-lhs
  "Parse the left-hand side and returns an AST"
  [lhs] 
  (parse-expression 
   (if (operators (first lhs))
     lhs
     (cons 'and lhs)))) ; "and" is implied if a list of constraints are given without an operator.

(defn- cartesian-join [lists lst]
    (if (seq lists)
      (let [[h & t] lists]
        (mapcat 
         (fn [l]
           (map #(conj % l) (cartesian-join t lst)))
         h))
      [lst]))

(defn ast-to-dnf 
  "Convert an AST to disjunctive normal form."
  [ast] 
  (condp = (:type ast)
    ;; Individual conditions can return unchanged.
    :condition
    ast

    :test
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

        :test ast

        :and (ast-to-dnf {:type :or
                          :content (negate-grandchildren)})
        
        :or (ast-to-dnf {:type :and 
                         :content (negate-grandchildren)})))
    
    ;; For all others, recursively process the children.
    (let [children (map ast-to-dnf (:content ast))
           ;; Get all conjunctions, which will not conain any disjunctions since they were processed above.
          conjunctions (filter #(#{:and :condition :not} (:type %)) children)]
      

      ;; TODO: Nodes with only a single expression as a child can be flattened.      
      (condp = (:type ast)

        :and
        (let [disjunctions (map :content (filter #(#{:or} (:type %)) children))]
          (if (empty? disjunctions)
            {:type :and
             :content (vec children)}

            {:type :or 
             :content (vec (for [c (cartesian-join disjunctions conjunctions)] 
                          {:type :and
                           :content c}))}))
        :or
        ;; Merge all child disjunctions into a single list.
        (let [disjunctions (mapcat :content (filter #(#{:or} (:type %)) children))]
          {:type :or
           ;; Nested disjunctions can be merged into the parent disjunction. We
           ;; the simply append nested conjunctions to create our DNF.
           :content (vec (concat disjunctions conjunctions))})))))

(defn- conjunction-to-cond-seq
  "Convert a conjunction expression to a sequence of ICondition objects."
  [expression]

  (condp = (:type expression)    

    ;; Recurse to nested elements. We need only recurse on ands,
    ;; since we are in DNF with nots pushed to the leaves.
    :and (apply concat (map conjunction-to-cond-seq (:content expression)))      
    
    ;; The expression is a simple condition, so use it.
    :condition [(:content expression)]

    ;; The expression is a negation, so create a negated condition around the raw condition.
    :not [(->NegatedCondition (:content (first (:content expression))))]
    
    ;; The expression is a test.
    :test [(:content expression)]))

(defn- expression-to-cond-seqs 
  "Given a rule, returns a vector containing one or more sequences
   of ICondition objects that trigger the rule."
  [expression]
  (let [dnf-expression (ast-to-dnf expression)]

    (if (= :or (:type dnf-expression))
      ;; If the DNF form has a top-level or, process each disjunction separatel.
      (vec (map conjunction-to-cond-seq (:content dnf-expression)))

      ;; Otherwise, return a single item vector, since there is only one disjuction.
      [(conjunction-to-cond-seq dnf-expression)])))

(defn- add-beta-tree 
  "Adds a condition sequence to the tree structure of beta nodes."
  [cond-seq production beta-tree]
  ;; Build a nested key pointing to our produciton, and add it to the tree.
  (assoc-in beta-tree 
            (concat (map condition-key cond-seq) 
                    (if (:rhs production) ; Use the appropriate key for queries and productions.
                      [:production]
                      [:query])) 
            production))

(defn- add-to-shredded
  "Adds a production to the shredded rules."
  [shredded-rules production]
  (let [cond-seqs (expression-to-cond-seqs (:lhs production))
        all-conds (apply concat cond-seqs)]

    ;; Merge the beta portion of the network to the tree.
    {:beta-tree
     (reduce (fn [beta-tree cond-seq] 
                     (add-beta-tree cond-seq production beta-tree))
                   (:beta-tree shredded-rules)
                   cond-seqs)
     
     ;; Update our map of all conditions.
     :conditions 
     (into (:conditions shredded-rules) 
           (for [condition all-conds] 
             [(condition-key condition) condition]))

     ;; Preserve the rules used.
     :rules (if (:rhs production)
              (conj (:rules shredded-rules) production)
              (:rules shredded-rules))

     ;; Preserve the queries used.
     :queries (if (not (:rhs production))
                (conj (:queries shredded-rules) production)
                (:queries shredded-rules))
     }))

(defn shred-rules
  "Shred the given rules into separate conditions, allowing for analysis, 
   combination of duplicate logic, and assembly into a rule base.

   This function breaks up productions into distinct pieces. Specifically:
  
   First, we have a tree of beta nodes. The beta side of a Rete network (ignoring the alpha side for now)
   is logically a tree, with shared logic represented as shared branches on the tree.
   We model that here by having each condition from all of the rules map to a
   \"condition key\", and that key is used as a node in our beta tree. So if multiple
   rules have a common condition in a common point in a tree, they map to the same
   place and therefore the logic is shared, in line with the Rete algorithm.

   The \"beta-tree\" produced by this shredding logic is modeled as a series of
   nested maps, where the key to each nested map is the condition-key described above.
   The leaves in our beta tree are the production rules or queries themselves.

   Second, we maintain a map of the condition keys used in the beta tree to the conditions
   themselves. If multiple rules have equivalent conditions, they will have
   the same condition key, and hence will map to the same point of our conditions
   map so the same item and alpha node will be used for both.

   This shredding operation also preserves the rules and queries provided by the
   caller. This is useful if a consumer wants to get back to the original rules
   for some other use, such as combining them with others to create a new
   knowledge base."
  [productions]
  (reduce add-to-shredded 
          {:beta-tree {}
           :conditions {}
           :rules []
           :queries []} 
          productions))

(defn- compile-beta-node
  "Compile a given beta node with a condition key and children,
   returning tuple of [beta-node node-map], where node-map is
   a map of condition keys to a list of beta nodes requiring that condition."
  [condition-key children ancestor-binding-keys condition-map]

  (condp = condition-key

    :production
    (let [production (->ProductionNode children (:rhs children))]
      [production {:production [production]}])

    :query
    (let [query (->QueryNode children (:params children))]
      [query {:query [query]}])

    ;; The node is neither a production or query terminal node, so compile the condition.
    (let [condition (get condition-map condition-key)
          ;; Get the binding keys defined in the condition, if any.
          binding-keys (binding-keys condition)                        

          ;; The keys used for any join operations performed by this node.
          join-binding-keys (s/intersection ancestor-binding-keys binding-keys)

          ;; All keys for the current node and the ancestors. 
          all-binding-keys (s/union ancestor-binding-keys binding-keys)

          ;; Get the child [beta-node node-map] tuples
          child-tuples (for [[child-condition-key grandchildren] children]
                         (compile-beta-node child-condition-key grandchildren all-binding-keys condition-map))

          ;; Get the child beta nodes and node maps themselves.
          child-nodes (map first child-tuples)
          child-maps (map second child-tuples)

          ;; Create a map to all children nodes by merging them together.
          node-map (apply merge-with concat child-maps)]
      
      (condp = (mode condition)

        :normal
        (let [join-node (->JoinNode condition child-nodes join-binding-keys)]
          [join-node (update-in node-map [condition-key] #(conj % join-node))])
        
        :accumulator
        (let [accumulate-node (->AccumulateNode condition (:definition condition) child-nodes join-binding-keys)]
          [accumulate-node (update-in node-map [condition-key] #(conj % accumulate-node))])

        ;; TODO: some inconsistency here in how we access the nested condition to be cleaned up.
        :negation
        (let [negation-node (->NegationNode (alpha-condition condition) child-nodes join-binding-keys)]
          [negation-node (update-in node-map [condition-key] #(conj % negation-node))])
        
        :test
        (let [test-node (->TestNode condition child-nodes)]
          [test-node (update-in node-map [condition-key] #(conj % test-node))])))))

(defn- compile-alpha-nodes
  "Compiles alpha nodes."
  [beta-node-map conditions]

  (for [[condition-key condition] conditions

        ;; Test conditions have no alpha node, so exclude them.
        :when (not (= :test (mode condition)))

        ;; Find all beta nodes that use the given condition.
        :let [beta-nodes (get beta-node-map condition-key)]]

    ;; Create the alpha node with corresponding beta node children.
    (->AlphaNode (alpha-condition condition) beta-nodes (activate-fn condition))))

(defn- node-id 
  "Generates hash functions for each node for use as short identifiers."
  [node]
  (->> node
       (tree-seq 
        #(or (sequential? %) (map? %) (set? %))
        (fn [item]
          (if (map? item)
            (concat (keys item) (vals item))
            item)))
       (filter #(or (string? %) (keyword? %) (symbol? %) (number? %)))
       (hash)))

(defn- node-id-map 
  "Generates a map of unique ids to nodes."
  [beta-roots]
  (let [beta-nodes (distinct
                    (mapcat 
                     #(tree-seq :children :children %)
                     beta-roots))
        items (into {} (for [beta-node beta-nodes] [(node-id beta-node) beta-node]))]
    items))

(defn compile-shredded-rules 
  "Compile shredded rules into a digraph representing the rule base, sharing nodes when possible"
  [shredded-rules] (let [compiled-betas (for [[condition-key children] (:beta-tree shredded-rules)]
                         (compile-beta-node condition-key children [] (:conditions shredded-rules)))

        beta-roots (map first compiled-betas)
        child-maps (map second compiled-betas)
        beta-node-map (apply merge-with concat child-maps)
        alpha-nodes (compile-alpha-nodes beta-node-map (:conditions shredded-rules)) 

        ;; Group alpha nodes by their type for use in the rulebase.
        alpha-map (group-by #(get-in % [:condition :type]) alpha-nodes)

        ;; Get a map of queries to the corresponding nodes.
        query-map (into {} (for [query-node (:query beta-node-map)]
                             [(:query query-node) query-node]))

        id-to-node (node-id-map beta-roots) ; TODO: use the beta node map instead of parsing again?
        node-to-id (s/map-invert id-to-node)]
   
    (map->Rulebase 
       {:alpha-roots alpha-map
        :beta-roots beta-roots
        :productions (:rules shredded-rules)
        :queries (:queries shredded-rules)
        :production-nodes (:production beta-node-map)
        :query-nodes query-map
        :node-to-id node-to-id
        :id-to-node id-to-node})))

(defn conj-rulebases 
  "Conjoin two rulebases, returning a new one with the same rules."
  [base1 base2]
  (-> (concat (:queries base1) (:queries base2) 
              (:productions base1) (:productions base2))
      (shred-rules)
      (compile-shredded-rules)))

;; Active session during rule execution.
(def ^:dynamic *current-session* nil)

;; The token that triggered a rule to fire.
(def ^:dynamic *rule-context* nil)

(defn fire-rules* 
   "Fire rules for the given nodes."
   [rulebase nodes transient-memory transport]
   (binding [*current-session* {:rulebase rulebase 
                                :transient-memory transient-memory 
                                :transport transport
                                :insertions (atom 0)}]

     (loop [insertion-count 0]
       (doseq [node nodes
               token (get-tokens transient-memory node {})]

         ;; Fire the node if it has not already been done for the token.
         (when (not (is-fired-token transient-memory node token))
           (binding [*rule-context* {:token token :node node}]
             ((:rhs node) token)
             
             ;; The rule fired for the given token, so mark it as such.
             (mark-as-fired! transient-memory node token))))
         
       ;; If the rules inserted new facts, re-fire to ensure they are accounted for.
       (when (> (deref (:insertions *current-session*)) insertion-count)
         (recur (deref (:insertions *current-session*)))))))

(deftype LocalSession [rulebase memory transport]
  ISession
  (insert [session facts]
    (let [transient-memory (to-transient memory)]
      (doseq [[cls fact-group] (group-by class facts)
              ancestor (conj (ancestors cls) cls) ; Find alpha nodes that match the class or any ancestor
              root (get-in rulebase [:alpha-roots ancestor])]
        (alpha-activate root fact-group transient-memory transport))
      (LocalSession. rulebase (to-persistent! transient-memory) transport)))

  (retract [session facts]

    (let [transient-memory (to-transient memory)]
      (doseq [[cls fact-group] (group-by class facts) 
              root (get-in rulebase [:alpha-roots cls])]
        (alpha-retract root fact-group transient-memory transport))

      (LocalSession. rulebase (to-persistent! transient-memory) transport)))

  (fire-rules [session]

    (let [transient-memory (to-transient memory)]
      (fire-rules* rulebase 
                   (:production-nodes rulebase)
                   transient-memory
                   transport)

      (LocalSession. rulebase (to-persistent! transient-memory) transport)))

  ;; TODO: queries shouldn't require the use of transient memory.
  (query [session query params]
    (let [query-node (get-in rulebase [:query-nodes query])]
      (when (= nil query-node) 
        (throw (IllegalArgumentException. "The given query is invalid or not included in the rule base.")))
      (map :bindings (get-tokens (to-transient (working-memory session)) query-node params))))
  
  (working-memory [session] memory))


(defn local-memory 
  "Returns a local, in-process working memory."
  [rulebase transport]     
  (let [memory (to-transient (->PersistentLocalMemory rulebase {}))]
    (doseq [beta-node (:beta-roots rulebase)]
      (left-activate beta-node {} [empty-token] memory transport))
    (to-persistent! memory)))

(defn print-memory 
  "Prints the session memory, usually for troubleshooting, and returns the session."
  [session]
  (pprint 
   (for [[k v] (:content (working-memory session))
         :let [node-id (get k 1)
               id-to-node (:id-to-node (:rulebase session))
               node-descrip (description (id-to-node node-id))]]
     [(assoc k 1 node-descrip) v]))

  session)
