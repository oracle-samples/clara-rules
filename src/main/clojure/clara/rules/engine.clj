(ns clara.rules.engine
  "The Clara rules engine. Most users should use only the clara.rules namespace."
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [clojure.set :as s]
            [clojure.string :as string]
            [clara.rules.memory :as mem]
            [clara.rules.platform :as platform]))


;; The accumulator is a Rete extension to run an accumulation (such as sum, average, or similar operation)
;; over a collection of values passing through the Rete network. This object defines the behavior
;; of an accumulator. See the AccumulatorNode for the actual node implementation in the network.
(defrecord Accumulator [input-condition initial-value reduce-fn combine-fn convert-return-fn])

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
(defrecord AccumulateNode [id accumulator result-binding children binding-keys]
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
        (platform/throw-error (str "The query " query " is invalid or not included in the rule base.")))
      (map :bindings (mem/get-tokens (mem/to-transient (working-memory session)) query-node params))))

  (working-memory [session] memory))


(defn local-memory
  "Returns a local, in-process working memory."
  [rulebase transport]
  (let [memory (mem/to-transient (mem/->PersistentLocalMemory rulebase {}))]
    (doseq [beta-node (:beta-roots rulebase)]
      (left-activate beta-node {} [empty-token] memory transport))
    (mem/to-persistent! memory)))

