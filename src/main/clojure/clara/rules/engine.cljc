(ns clara.rules.engine
  "This namespace is for internal use and may move in the future. Most users should use only the clara.rules namespace."
  (:require [clojure.reflect :as reflect]
            [clojure.core.reducers :as r]
            [schema.core :as s]
            [clojure.string :as string]
            [clara.rules.memory :as mem]
            [clara.rules.listener :as l]
            [clara.rules.platform :as platform]))

;; The accumulator is a Rete extension to run an accumulation (such as sum, average, or similar operation)
;; over a collection of values passing through the Rete network. This object defines the behavior
;; of an accumulator. See the AccumulatorNode for the actual node implementation in the network.
(defrecord Accumulator [input-condition initial-value reduce-fn combine-fn convert-return-fn])

;; A Rete-style token, which contains two items:
;; * matches, a sequence of [fact, node-id] tuples for the facts and corresponding nodes they matched.
;; * bindings, a map of keyword-to-values for bound variables.
(defrecord Token [matches bindings])

;; A working memory element, containing a single fact and its corresponding bound variables.
(defrecord Element [fact bindings])

;; An activation for the given production and token.
(defrecord Activation [node token])

;; Token with no bindings, used as the root of beta nodes.
(def empty-token (->Token [] {}))

;; Record indicating the negation existing in the working memory.
(defrecord NegationResult [gen-rule-name])

;; Make the negation result a "system type", so its type is not overridden
;; with a customized fact type function.
(derive NegationResult :clara.rules.engine/system-type)

;; Schema for the structure returned by the components
;; function on the session protocol.
;; This is simply a comment rather than first-class schema
;; for now since it's unused for validation and created
;; undesired warnings as described at https://groups.google.com/forum/#!topic/prismatic-plumbing/o65PfJ4CUkI
(comment

  (def session-components-schema
    {:rulebase s/Any
     :memory s/Any
     :transport s/Any
     :listeners [s/Any]
     :get-alphas-fn s/Any}))

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

  ;; Returns the components of a session as defined in the session-components-schema
  (components [session]))

;; Left activation protocol for various types of beta nodes.
(defprotocol ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener])
  (left-retract [node join-bindings tokens memory transport listener])
  (description [node])
  (get-join-keys [node]))

;; Right activation protocol to insert new facts, connecting alpha nodes
;; and beta nodes.
(defprotocol IRightActivate
  (right-activate [node join-bindings elements memory transport listener])
  (right-retract [node join-bindings elements memory transport listener]))

;; Specialized right activation interface for accumulator nodes,
;; where the caller has the option of pre-reducing items
;; to reduce the data sent to the node. This would be useful
;; if the caller is not in the same memory space as the accumulator node itself.
(defprotocol IAccumRightActivate
  ;; Pre-reduces elements, returning a map of bindings to reduced elements.
  (pre-reduce [node elements])

  ;; Right-activate the node with items reduced in the above pre-reduce step.
  (right-activate-reduced [node join-bindings reduced  memory transport listener]))

;; The transport protocol for sending and retracting items between nodes.
(defprotocol ITransport
  (send-elements [transport memory listener nodes elements])
  (send-tokens [transport memory listener nodes tokens])
  (retract-elements [transport memory listener nodes elements])
  (retract-tokens [transport memory listener nodes tokens]))

(defn- propagate-items-to-nodes [transport memory listener nodes items propagate-fn]
  (doseq [node nodes
          :let [join-keys (get-join-keys node)]]

    (if (pos? (count join-keys))

      ;; Group by the join keys for the activation.
      (doseq [[join-bindings item-group] (platform/group-by-seq #(select-keys (:bindings %) join-keys) items)]
        (propagate-fn node
                      join-bindings
                      item-group
                      memory
                      transport
                      listener))

      ;; The node has no join keys, so just send everything at once
      ;; (if there is something to send.)
      (when (seq items)
        (propagate-fn node
                      {}
                      items
                      memory
                      transport
                      listener)))))

;; Simple, in-memory transport.
(deftype LocalTransport []
  ITransport
  (send-elements [transport memory listener nodes elements]
    (propagate-items-to-nodes transport memory listener nodes elements right-activate))

  (send-tokens [transport memory listener nodes tokens]
    (propagate-items-to-nodes transport memory listener nodes tokens left-activate))

  (retract-elements [transport memory listener nodes elements]
    (propagate-items-to-nodes transport memory listener nodes elements right-retract))

  (retract-tokens [transport memory listener nodes tokens]
    (propagate-items-to-nodes transport memory listener nodes tokens left-retract)))

;; Protocol for activation of Rete alpha nodes.
(defprotocol IAlphaActivate
  (alpha-activate [node facts memory transport listener])
  (alpha-retract [node facts memory transport listener]))

;; Record indicating pending insertion or removal of a sequence of facts.
(defrecord PendingUpdate [type facts])

;; Active session during rule execution.
(def ^:dynamic *current-session* nil)

;; The token that triggered a rule to fire.
(def ^:dynamic *rule-context* nil)

(defn- flush-updates
  "Flush all pending updates in the current session. Returns true if there were
   some items to flush, false otherwise"
  [current-session]
  (letfn [(flush-all [current-session flushed-items?]
            (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} current-session
                  pending-updates @(:pending-updates current-session)]

              ;; Remove the facts here so they are re-inserted if we flush recursively.
              (reset! (:pending-updates current-session) [])

              (if (empty? pending-updates)
                flushed-items?
                (do
                  (doseq [partition (partition-by :type pending-updates)
                          :let [facts (mapcat :facts partition)]
                          [alpha-roots fact-group] (get-alphas-fn facts)
                          root alpha-roots]

                    (if (= :insert (:type (first partition)))
                      (alpha-activate root fact-group transient-memory transport listener)
                      (alpha-retract root fact-group transient-memory transport listener)))

                  ;; There may be new :pending-updates due to the flush just
                  ;; made.  So keep flushing until there are none left.  Items
                  ;; were flushed though, so flush-items? is now true.
                  (flush-all current-session true)))))]

    (flush-all current-session false)))

(defn insert-facts!
  "Place facts in a stateful cache to be inserted into the session 
  immediately after the RHS of a rule fires."
  [facts unconditional]
  (if unconditional
    (swap! (:batched-unconditional-insertions *rule-context*) into facts)
    (swap! (:batched-logical-insertions *rule-context*) into facts)))

(defn rhs-retract-facts!
  "Place all facts retracted in the RHS in a buffer to be retracted after
   the eval'ed RHS function completes."
  [facts]
  (swap! (:batched-rhs-retractions *rule-context*) into facts))

(defn ^:private flush-rhs-retractions!
  "Retract all facts retracted in the RHS after the eval'ed RHS function completes.
  This should only be used for facts explicitly retracted in a RHS.
  It should not be used for retractions that occur as part of automatic truth maintenance."
  [facts]
  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} *current-session*]

    ;; Update the count so the rule engine will know when we have normalized.
    (swap! insertions + (count facts))

    (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
            root alpha-roots]

      (when listener
        (l/retract-facts! listener fact-group))
      (alpha-retract root fact-group transient-memory transport listener))))

(defn ^:private flush-insertions!
  "Perform the actual fact insertion, optionally making them unconditional.  This should only
   be called once per rule activation for logical insertions."
  [facts unconditional]
  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} *current-session*
        {:keys [node token]} *rule-context*]

    ;; Update the insertion count.
    (swap! insertions + (count facts))

    ;; Track this insertion in our transient memory so logical retractions will remove it.
    (if unconditional
      (l/insert-facts! listener facts)
      (do
        (mem/add-insertions! transient-memory node token facts)
        (l/insert-facts-logical! listener node token facts)))

    (swap! (:pending-updates *current-session*) into [(->PendingUpdate :insert facts)])))

(defn retract-facts!
  "Perform the fact retraction."
  [facts]
  (swap! (:pending-updates *current-session*) into [(->PendingUpdate :retract facts)]))

;; Record for the production node in the Rete network.
(defrecord ProductionNode [id production rhs]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]

    (l/left-activate! listener node tokens)

    ;; Fire the rule if it's not a no-loop rule, or if the rule is not
    ;; active in the current context.
    (when (or (not (get-in production [:props :no-loop]))
              (not (= production (get-in *rule-context* [:node :production]))))

      ;; Preserve tokens that fired for the rule so we
      ;; can perform retractions if they become false.
      (mem/add-tokens! memory node join-bindings tokens)

      (let [activations (for [token tokens]
                          (->Activation node token))]

        (l/add-activations! listener node activations)

        ;; The production matched, so add the tokens to the activation list.
        (mem/add-activations! memory production activations))))

  (left-retract [node join-bindings tokens memory transport listener]

    (l/left-retract! listener node tokens)

    ;; Remove any tokens to avoid future rule execution on retracted items.
    (mem/remove-tokens! memory node join-bindings tokens)

    ;; Remove pending activations triggered by the retracted tokens.
    (let [activations (for [token tokens]
                        (->Activation node token))]

      (l/remove-activations! listener node activations)
      (mem/remove-activations! memory production activations))

    ;; Retract any insertions that occurred due to the retracted token.
    (let [token-insertion-map (mem/remove-insertions! memory node tokens)]

      (when-let [insertions (seq (apply concat (vals token-insertion-map)))]
        ;; If there is current session with rules firing, add these items to the queue
        ;; to be retracted so they occur in the same order as facts being inserted.
        (if *current-session*
          ;; Retract facts that have become untrue, unless they became untrue
          ;; because of an activation of the current rule that is :no-loop
          (when (or (not (get-in production [:props :no-loop]))
                    (not (= production (get-in *rule-context* [:node :production]))))
            (do
              ;; Notify the listener of logical retractions.
              (doseq [[token token-insertions] token-insertion-map]
                (l/retract-facts-logical! listener node token token-insertions))
              (retract-facts! insertions)))

          ;; The retraction is occuring outside of a rule-firing phase,
          ;; so simply retract them as an external caller would.
          (let [get-alphas-fn (mem/get-alphas-fn memory)]
            ;; Notify the listener of logical retractions.
            (doseq [[token token-insertions] token-insertion-map]
              (l/retract-facts-logical! listener node token token-insertions))
            (doseq [[alpha-roots fact-group] (get-alphas-fn insertions)
                    root alpha-roots]
              (alpha-retract root fact-group memory transport listener)))))))

  (get-join-keys [node] [])

  (description [node] "ProductionNode"))

;; The QueryNode is a terminal node that stores the
;; state that can be queried by a rule user.
(defrecord QueryNode [id query param-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    (l/left-activate! listener node tokens)
    (mem/add-tokens! memory node join-bindings tokens))

  (left-retract [node join-bindings tokens memory transport listener]
    (l/left-retract! listener node tokens)
    (mem/remove-tokens! memory node join-bindings tokens))

  (get-join-keys [node] param-keys)

  (description [node] (str "QueryNode -- " query)))

;; Record representing alpha nodes in the Rete network,
;; each of which evaluates a single condition and
;; propagates matches to its children.
(defrecord AlphaNode [env children activation]
  IAlphaActivate
  (alpha-activate [node facts memory transport listener]
    (send-elements
     transport
     memory
     listener
     children
     (for [fact facts
           :let [bindings (activation fact env)] :when bindings] ; FIXME: add env.
       (->Element fact bindings))))

  (alpha-retract [node facts memory transport listener]

    (retract-elements
     transport
     memory
     listener
     children
     (for [fact facts
           :let [bindings (activation fact env)] :when bindings] ; FIXME: add env.
       (->Element fact bindings)))))

(defrecord RootJoinNode [id condition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; This specialized root node doesn't need to deal with the
    ;; empty token, so do nothing.
    )

  (left-retract [node join-bindings tokens memory transport listener]
    ;; The empty token can't be retracted from the root node,
    ;; so do nothing.
    )

  (get-join-keys [node] binding-keys)

  (description [node] (str "RootJoinNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]

    (l/right-activate! listener node elements)


    ;; Add elements to the working memory to support analysis tools.
    (mem/add-elements! memory node join-bindings elements)
    ;; Simply create tokens and send it downstream.
    (send-tokens
     transport
     memory
     listener
     children
     (for [{:keys [fact bindings] :as element} elements]
       (->Token [[fact (:id node)]] bindings))))

  (right-retract [node join-bindings elements memory transport listener]

    (l/right-retract! listener node elements)

    ;; Remove matching elements and send the retraction downstream.
    (retract-tokens
     transport
     memory
     listener
     children
     (for [{:keys [fact bindings] :as element} (mem/remove-elements! memory node join-bindings elements)]
       (->Token [[fact (:id node)]] bindings)))))

;; Record for the join node, a type of beta node in the rete network. This node performs joins
;; between left and right activations, creating new tokens when joins match and sending them to
;; its descendents.
(defrecord HashJoinNode [id condition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)
    (send-tokens
     transport
     memory
     listener
     children
     (for [element (mem/get-elements memory node join-bindings)
           token tokens
           :let [fact (:fact element)
                 fact-binding (:bindings element)]]
       (->Token (conj (:matches token) [fact id]) (conj fact-binding (:bindings token))))))

  (left-retract [node join-bindings tokens memory transport listener]
    (retract-tokens
     transport
     memory
     listener
     children
     (for [token (mem/remove-tokens! memory node join-bindings tokens)
           element (mem/get-elements memory node join-bindings)
           :let [fact (:fact element)
                 fact-bindings (:bindings element)]]
       (->Token (conj (:matches token) [fact id]) (conj fact-bindings (:bindings token))))))

  (get-join-keys [node] binding-keys)

  (description [node] (str "JoinNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (mem/add-elements! memory node join-bindings elements)
    (send-tokens
     transport
     memory
     listener
     children
     (for [token (mem/get-tokens memory node join-bindings)
           {:keys [fact bindings] :as element} elements]
       (->Token (conj (:matches token) [fact id]) (conj (:bindings token) bindings)))))

  (right-retract [node join-bindings elements memory transport listener]
    (retract-tokens
     transport
     memory
     listener
     children
     (for [{:keys [fact bindings] :as element} (mem/remove-elements! memory node join-bindings elements)
           token (mem/get-tokens memory node join-bindings)]
       (->Token (conj (:matches token) [fact id]) (conj (:bindings token) bindings))))))


(defrecord ExpressionJoinNode [id condition join-filter-fn children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)
    (send-tokens
     transport
     memory
     listener
     children
     (for [element (mem/get-elements memory node join-bindings)
           token tokens
           :let [fact (:fact element)
                 fact-binding (:bindings element)
                 beta-bindings (join-filter-fn token fact {})]
           :when beta-bindings]
       (->Token (conj (:matches token) [fact id])
                (conj fact-binding (:bindings token) beta-bindings)))))

  (left-retract [node join-bindings tokens memory transport listener]
    (retract-tokens
     transport
     memory
     listener
     children
     (for [token (mem/remove-tokens! memory node join-bindings tokens)
           element (mem/get-elements memory node join-bindings)
           :let [fact (:fact element)
                 fact-bindings (:bindings element)
                 beta-bindings (join-filter-fn token fact {})]
           :when beta-bindings]
       (->Token (conj (:matches token) [fact id])
                (conj fact-bindings (:bindings token) beta-bindings)))))

  (get-join-keys [node] binding-keys)

  (description [node] (str "JoinNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (mem/add-elements! memory node join-bindings elements)
    (send-tokens
     transport
     memory
     listener
     children
     (for [token (mem/get-tokens memory node join-bindings)
           {:keys [fact bindings] :as element} elements
           :let [beta-bindings (join-filter-fn token fact {})]
           :when beta-bindings]
       (->Token (conj (:matches token) [fact id])
                (conj (:bindings token) bindings beta-bindings)))))

  (right-retract [node join-bindings elements memory transport listener]
    (retract-tokens
     transport
     memory
     listener
     children
     (for [{:keys [fact bindings] :as element} (mem/remove-elements! memory node join-bindings elements)
           token (mem/get-tokens memory node join-bindings)
           :let [beta-bindings (join-filter-fn token fact {})]
           :when beta-bindings]
       (->Token (conj (:matches token) [fact id])
                (conj (:bindings token) bindings beta-bindings))))))

;; The NegationNode is a beta node in the Rete network that simply
;; negates the incoming tokens from its ancestors. It sends tokens
;; to its descendent only if the negated condition or join fails (is false).
(defrecord NegationNode [id condition children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)
    (when (empty? (mem/get-elements memory node join-bindings))
      (send-tokens transport memory listener children tokens)))

  (left-retract [node join-bindings tokens memory transport listener]
    (mem/remove-tokens! memory node join-bindings tokens)
    (when (empty? (mem/get-elements memory node join-bindings))
      (retract-tokens transport memory listener children tokens)))

  (get-join-keys [node] binding-keys)

  (description [node] (str "NegationNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (mem/add-elements! memory node join-bindings elements)
    ;; Retract tokens that matched the activation, since they are no longer negatd.
    (retract-tokens transport memory listener children (mem/get-tokens memory node join-bindings)))

  (right-retract [node join-bindings elements memory transport listener]
    (mem/remove-elements! memory node join-bindings elements)
    (when (empty? (mem/get-elements memory node join-bindings))
      (send-tokens transport memory listener children (mem/get-tokens memory node join-bindings)))))

(defn- matches-some-facts?
  "Returns true if the given token matches one or more of the given elements."
  [token elements join-filter-fn condition]
  (some (fn [{:keys [fact]}]
          (join-filter-fn token fact (:env condition)))
        elements))

;; A specialization of the NegationNode that supports additional tests
;; that have to occur on the beta side of the network. The key difference between this and the simple
;; negation node is the join-filter-fn, which allows negation tests to
;; be applied with the parent token in context, rather than just a simple test of the non-existence
;; on the alpha side.
(defrecord NegationWithJoinFilterNode [id condition join-filter-fn children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)

    (send-tokens transport
                 memory
                 listener
                 children
                 (for [token tokens
                       :when (not (matches-some-facts? token
                                                       (mem/get-elements memory node join-bindings)
                                                       join-filter-fn
                                                       condition))]
                   token)))

  (left-retract [node join-bindings tokens memory transport listener]
    (mem/remove-tokens! memory node join-bindings tokens)
    (retract-tokens transport
                    memory
                    listener
                    children

                    ;; Retract only if it previously had no matches in the negation node,
                    ;; and therefore had an activation.
                    (for [token tokens
                          :when (not (matches-some-facts? token
                                                          (mem/get-elements memory node join-bindings)
                                                          join-filter-fn
                                                          condition))]
                      token)))

  (get-join-keys [node] binding-keys)

  (description [node] (str "NegationWithJoinFilterNode -- " (:text condition)))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (mem/add-elements! memory node join-bindings elements)
    ;; Retract tokens that matched the activation, since they are no longer negated.
    (retract-tokens transport
                    memory
                    listener
                    children
                    (for [token (mem/get-tokens memory node join-bindings)

                          :when (matches-some-facts? token
                                                     elements
                                                     join-filter-fn
                                                     condition)]
                      token)))

  (right-retract [node join-bindings elements memory transport listener]
    (mem/remove-elements! memory node join-bindings elements)

    (send-tokens transport
                 memory
                 listener
                 children
                 (for [token (mem/get-tokens memory node join-bindings)

                       ;; Propagate tokens when some of the retracted facts joined
                       ;; but none of the remaining facts do.
                       :when (and (matches-some-facts? token
                                                     elements
                                                     join-filter-fn
                                                     condition)
                                  (not (matches-some-facts? token
                                                            (mem/get-elements memory node join-bindings)
                                                            join-filter-fn
                                                            condition)))]
                   token))))

;; The test node represents a Rete extension in which
(defrecord TestNode [id test children]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    (send-tokens
     transport
     memory
     listener
     children
     (filter test tokens)))

  (left-retract [node join-bindings tokens memory transport listener]
    (retract-tokens transport memory listener children tokens))

  (get-join-keys [node] [])

  (description [node] (str "TestNode -- " (:text test))))

(defn- do-accumulate
  "Runs the actual accumulation.  Returns the accumulated value."
  [accumulator facts]
  (r/reduce (:reduce-fn accumulator)
            (:initial-value accumulator)
            facts))

(defn- retract-accumulated
  "Helper function to retract an accumulated value."
  [node accum-condition accumulator result-binding token converted-result fact-bindings transport memory listener]
  (let [new-facts (conj (:matches token) [converted-result (:id node)])
        new-bindings (merge (:bindings token)
                            fact-bindings
                            (when result-binding
                              { result-binding
                                converted-result}))]

    (retract-tokens transport memory listener (:children node)
                    [(->Token new-facts new-bindings)])))

(defn- send-accumulated
  "Helper function to send the result of an accumulated value to the node's children."
  [node accum-condition accumulator result-binding token converted-result fact-bindings transport memory listener]
  (let [new-bindings (merge (:bindings token)
                            fact-bindings
                            (when result-binding
                              { result-binding
                               converted-result}))

        ;; This is to check that the produced accumulator result is
        ;; consistent with any variable from another rule condition
        ;; that has the same binding. If another condition binds something
        ;; to ?x, only the accumulator results that match that would propagate.
        ;; We can do this safely because previous states get retracted.
        previous-result (get (:bindings token) result-binding ::no-previous-result)]

    (when (or (= previous-result ::no-previous-result)
              (= previous-result converted-result))

      (send-tokens transport memory listener (:children node)
                   [(->Token (conj (:matches token) [converted-result (:id node)]) new-bindings)]))))

;; The AccumulateNode hosts Accumulators, a Rete extension described above, in the Rete network.
;; It behaves similarly to a JoinNode, but performs an accumulation function on the incoming
;; working-memory elements before sending a new token to its descendents.
(defrecord AccumulateNode [id accum-condition accumulator result-binding children binding-keys new-bindings]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    (let [previous-results (mem/get-accum-reduced-all memory node join-bindings)
          convert-return-fn (:convert-return-fn accumulator)
          has-matches? (seq previous-results)
          initial-value (when-not has-matches?
                          (:initial-value accumulator))
          initial-converted (when (some? initial-value)
                              (convert-return-fn initial-value))]

      (mem/add-tokens! memory node join-bindings tokens)
      
      (cond
        ;; If there are previously accumulated results to propagate, use them.  If
        ;; this is the first time there are matching tokens, then the reduce will
        ;; have to happen for the first time.  However, this reduce operation is
        ;; independent of the specific tokens since the elements join to the tokens
        ;; via pre-computed hash join bindings for this node.  So only reduce once,
        ;; for all tokens.
        has-matches?
        (doseq [:let [;; There is at least one previous result due to has-matches?.
                      [_ [previous previous-reduced]] (first previous-results)
                      first-reduce? (= ::not-reduced previous-reduced)
                      previous-reduced (if first-reduce?
                                         ;; Need to accumulate since this is the first time we have
                                         ;; tokens matching so we have not accumulated before.
                                         (do-accumulate accumulator previous)
                                         previous-reduced)
                      accum-reduced (when first-reduce?
                                      [previous previous-reduced])
                      converted (when (some? previous-reduced)
                                  (convert-return-fn previous-reduced))]
                [fact-bindings [previous previous-reduced]] previous-results]
          
          ;; Newly accumulated results need to be added to memory.
          (when first-reduce?
            (l/add-accum-reduced! listener node join-bindings accum-reduced fact-bindings)
            (mem/add-accum-reduced! memory node join-bindings accum-reduced fact-bindings))

          (when (some? converted)
            (doseq [token tokens]
              (send-accumulated node accum-condition accumulator result-binding token converted fact-bindings
                                transport memory listener))))

        ;; There are no previously accumulated results, but we still may need to propagate things
        ;; such as a sum of zero items.
        ;; If an initial value is provided and the converted value is non-nil, we can propagate
        ;; the converted value as the accumulated item.
        (and (some? initial-converted)
             (empty? new-bindings))

        ;; Note that this is added to memory a single time for all matching tokens because the memory
        ;; location doesn't depend on bindings from individual tokens.
        (let [accum-reduced [[] initial-value]]
          ;; The fact-bindings are normally a superset of the join-bindings.  We have no fact-bindings
          ;; that are not join-bindings in this case since we have verified that new-bindings is empty.
          ;; Therefore the join-bindings and fact-bindings are exactly equal.
          (l/add-accum-reduced! listener node join-bindings accum-reduced join-bindings)
          (mem/add-accum-reduced! memory node join-bindings accum-reduced join-bindings)
          
          ;; Send the created accumulated item to the children for each token.
          (doseq [token tokens]
            (send-accumulated node accum-condition accumulator result-binding token initial-converted {}
                              transport memory listener)))
        
        ;; Propagate nothing if the above conditions don't apply.
        :else
        nil)))

  (left-retract [node join-bindings tokens memory transport listener]
    (doseq [:let [previous-results (mem/get-accum-reduced-all memory node join-bindings)
                  ;; If there are any previous results we know we have an actual previous reduced
                  ;; value because we have tokens to remove.  This means there were matches before
                  ;; and a value is always accumulated and propagated when there are matches.
                  ;; The converted value only needs to be calculated once for all token + element
                  ;; pairs retracted.
                  [_ [_ previous-reduced]] (first previous-results)
                  previous-converted (when (some? previous-reduced)
                                       ((:convert-return-fn accumulator) previous-reduced))
                  removed-tokens (mem/remove-tokens! memory node join-bindings tokens)
                  remaining-tokens (mem/get-tokens memory node join-bindings)

                  ;; If there are no new bindings created by the accumulator condition then
                  ;; a left-activation can create a new binding group in the accumulator memory.
                  ;; If this token is later removed without corresponding elements having been added,
                  ;; we remove the binding group from the accum memory.  Otherwise adding and then retracting
                  ;; tokens could force bindings to retained for the duration of the JVM, regardless of whether
                  ;; the backing facts were garbage collectable.  This would be a memory leak.
                  _ (when (and (empty? remaining-tokens)
                               (empty? new-bindings)
                               (let [current (mem/get-accum-reduced memory node join-bindings join-bindings)]
                                 (and
                                  ;; If there is nothing under these bindings already in the memory then there is no
                                  ;; need to take further action.
                                  (not= current ::mem/no-accum-reduced)
                                  ;; Check to see if there are elements under this binding group.
                                  ;; If elements are present we must keep the binding group regardless of the
                                  ;; presence or absence of tokens.
                                  (-> current first empty?))))
                      (mem/remove-accum-reduced! memory node join-bindings join-bindings))]
            
            token removed-tokens
            ;; A nil previous result should not have been propagated before.
            :when (some? previous-converted)
            ;; Note that this will cause a Cartesian join between tokens and elements groups where the token
            ;; and element group share the same join bindings, but the element groups may have additional bindings
            ;; that come from their alpha nodes. Keep in mind that these element groups need elements to be created
            ;; and cannot come from initial values if they have bindings that are not shared with tokens.
            [fact-bindings [previous previous-reduced]] previous-results]
      (retract-accumulated node accum-condition accumulator result-binding token previous-converted fact-bindings
                           transport memory listener)))

  (get-join-keys [node] binding-keys)

  (description [node] (str "AccumulateNode -- " accumulator))

  IAccumRightActivate
  (pre-reduce [node elements]
    ;; Return a seq tuples with the form [binding-group facts-from-group-elements].
    (for [[bindings element-group] (platform/group-by-seq :bindings elements)]
      [bindings (mapv :fact element-group)]))

  (right-activate-reduced [node join-bindings fact-seq memory transport listener]
    
    ;; Combine previously reduced items together, join to matching tokens, and emit child tokens.
    (doseq [:let [convert-return-fn (:convert-return-fn accumulator)
                  ;; Note that we want to iterate over all tokens with the desired join bindings later
                  ;; independently of the fact binding groups created by elements; that is, a token
                  ;; can join with multiple groups of fact bindings when the accumulator condition
                  ;; creates new bindings.
                  matched-tokens (mem/get-tokens memory node join-bindings)
                  has-matches? (seq matched-tokens)]
            [bindings facts] fact-seq
            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)
                  has-previous? (not= ::mem/no-accum-reduced previous)
                  [previous previous-reduced] (if has-previous?
                                                previous
                                                [::mem/no-accum-reduced ::not-reduced])
                  combined (if has-previous?
                             (into previous facts)
                             facts)
                  combined-reduced
                  (cond
                    ;; Reduce all of the combined items for the first time if there are
                    ;; now matches, and nothing was reduced before.
                    (and has-matches?
                         (= ::not-reduced previous-reduced))
                    (do-accumulate accumulator combined)

                    ;; There are matches, a previous reduced value for the previous items and a
                    ;; :combine-fn is given.  Use the :combine-fn on both the previously reduced
                    ;; and the newly reduced results.
                    (and has-matches?
                         (:combine-fn accumulator))
                    ((:combine-fn accumulator) previous-reduced (do-accumulate accumulator facts))

                    ;; There are matches and there is a previous reduced value for the previous
                    ;; items.  So just add the new items to the accumulated value.
                    has-matches?
                    (do-accumulate (assoc accumulator :initial-value previous-reduced) facts)

                    ;; There are no matches right now.  So do not perform any accumulations.
                    ;; If there are never matches, time will be saved by never reducing.
                    :else
                    ::not-reduced)
                  
                  converted (when (and (some? combined-reduced)
                                       (not= ::not-reduced combined-reduced))
                              (convert-return-fn combined-reduced))

                  previous-converted (when (and has-previous?
                                                (some? previous-reduced)
                                                (not= ::not-reduced previous-reduced))
                                       (convert-return-fn previous-reduced))
                  
                  accum-reduced [combined combined-reduced]]]

      ;; Add the combined results to memory.
      (l/add-accum-reduced! listener node join-bindings accum-reduced bindings)
      (mem/add-accum-reduced! memory node join-bindings accum-reduced bindings)

      (cond

        ;; Do nothing when the result was nil before and after.
        (and (nil? previous-converted)
             (nil? converted))
        nil

        (nil? converted)
        (doseq [token matched-tokens]
          (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                               transport memory listener))

        (nil? previous-converted)
        (doseq [token matched-tokens]
          (send-accumulated node accum-condition accumulator result-binding token converted bindings
                            transport memory listener))
        

        ;; If there are previous results, then propagate downstream if the new result differs from
        ;; the previous result.  If the new result is equal to the previous result don't do
        ;; anything.  Note that the memory has already been updated with the new combined value,
        ;; which may be needed if elements in memory changes later.
        (not= converted previous-converted)
        ;; There is no requirement that we doseq over all retractions then doseq over propagations; we could
        ;; just as easily doseq over tokens at the top level and retract and propagate for each token in turn.
        ;; In the absence of hard evidence either way, doing it this way is just an educated guess as to
        ;; which is likely to be more performant.
        (do
          (doseq [token matched-tokens]
            (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                                 transport memory listener))
          (doseq [token matched-tokens]
            (send-accumulated node accum-condition accumulator result-binding token converted bindings
                              transport memory listener))))))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]

    ;; Simple right-activate implementation simple defers to
    ;; accumulator-specific logic.
    (right-activate-reduced
     node
     join-bindings
     (pre-reduce node elements)
     memory
     transport
     listener))

  (right-retract [node join-bindings elements memory transport listener]

    (doseq [:let [convert-return-fn (:convert-return-fn accumulator)
                  ;; As in right-activate-reduced, a token can match with multiple groupings of elements
                  ;; by their bindings.
                  matched-tokens (mem/get-tokens memory node join-bindings)
                  has-matches? (seq matched-tokens)]
            [bindings elements] (platform/group-by-seq :bindings elements)

            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)
                  has-previous? (not= :clara.rules.memory/no-accum-reduced previous)
                  [previous previous-reduced] (if has-previous?
                                                previous
                                                [:clara.rules.memory/no-accum-reduced ::not-reduced])]

            ;; No need to retract anything if there were no previous items.
            :when has-previous?

            ;; Compute the new version with the retracted information.
            :let [facts (mapv :fact elements)
                  [removed retracted] (mem/remove-first-of-each facts previous)
                  all-retracted? (empty? retracted)
                  ;; If there is a previous and matches, there would have been a
                  ;; propagated and accumulated value.  So there is something to
                  ;; retract and re-accumulated in place of.
                  ;; Otherwise, no reduce is needed right now.
                  retracted-reduced (if (and has-matches?
                                             (not all-retracted?))
                                      ;; Use the provided :retract-fn if one is provided.
                                      ;; Otherwise, just re-accumulate based on the
                                      ;; remaining items after retraction.
                                      (if-let [retract-fn (:retract-fn accumulator)]
                                        (r/reduce retract-fn previous-reduced removed)
                                        (do-accumulate accumulator retracted))
                                      ::not-reduced)

                  retracted-converted (when (and (some? retracted-reduced)
                                                 (not= ::not-reduced retracted-reduced))
                                        (convert-return-fn retracted-reduced))
                  previous-converted (when (some? previous-reduced)
                                       (convert-return-fn previous-reduced))]]
      
      (if all-retracted?
        (do
          ;; When everything has been retracted we need to remove the accumulated results from memory.
          (l/remove-accum-reduced! listener node join-bindings bindings)
          (mem/remove-accum-reduced! memory node join-bindings bindings)

          (doseq [:when (some? previous-converted)
                  token matched-tokens]
            ;; Retract the previous token.
            (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                                 transport memory listener))
          
          (let [initial-value (:initial-value accumulator)
                
                initial-converted (when initial-value
                                    (convert-return-fn initial-value))]
            (when (and (some? initial-converted)
                       (empty? new-bindings))

              (doseq [token matched-tokens]
                (l/add-accum-reduced! listener node join-bindings [[] initial-value] join-bindings)
                (mem/add-accum-reduced! memory node join-bindings [[] initial-value] join-bindings)
                (send-accumulated node accum-condition accumulator result-binding token initial-converted {}
                                  transport memory listener)))))
        (do
          ;; Add our newly retracted information to our node.
          (l/add-accum-reduced! listener node join-bindings [retracted retracted-reduced] bindings)
          (mem/add-accum-reduced! memory node join-bindings [retracted retracted-reduced] bindings)

          (cond
            (and (nil? previous-converted)
                 (nil? retracted-converted))
            nil

            (nil? previous-converted)
            (doseq [token matched-tokens]
              (send-accumulated node accum-condition accumulator result-binding token retracted-converted bindings
                                transport memory listener))

            (nil? retracted-converted)
            (doseq [token matched-tokens]
              (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                                   transport memory listener))

            (not= retracted-converted previous-converted)
            ;; There is no requirement that we doseq over all retractions then doseq over propagations; we could
            ;; just as easily doseq over tokens at the top level and retract and propagate for each token in turn.
            ;; In the absence of hard evidence either way, doing it this way is just an educated guess as to
            ;; which is likely to be more performant.
            (do
              (doseq [token matched-tokens]
                (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings
                                     transport memory listener))
              (doseq [token matched-tokens]
                (send-accumulated node accum-condition accumulator result-binding token retracted-converted bindings
                                  transport memory listener)))))))))

(defn- filter-accum-facts
  "Run a filter on elements against a given token for constraints that are not simple hash joins."
  [join-filter-fn token candidate-facts]
  (filter #(join-filter-fn token % {}) candidate-facts))

;; A specialization of the AccumulateNode that supports additional tests
;; that have to occur on the beta side of the network. The key difference between this and the simple
;; accumulate node is the join-filter-fn, which accepts a token and a fact and filters out facts that
;; are not consistent with the given token.
(defrecord AccumulateWithJoinFilterNode [id accum-condition accumulator join-filter-fn
                                         result-binding children binding-keys new-bindings]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]

    ;; Facts that are candidates for matching the token are used in this accumulator node,
    ;; which must be filtered before running the accumulation.
    (let [convert-return-fn (:convert-return-fn accumulator)
          grouped-candidate-facts (mem/get-accum-reduced-all memory node join-bindings)]
      (mem/add-tokens! memory node join-bindings tokens)

      (cond

        (seq grouped-candidate-facts)
        (doseq [token tokens
                [fact-bindings candidate-facts] grouped-candidate-facts

                ;; Filter to items that match the incoming token, then apply the accumulator.
                :let [filtered-facts (filter-accum-facts join-filter-fn token candidate-facts)]

                :when (or (seq filtered-facts)
                          ;; Even if there no filtered facts, if there are no new bindings we may
                          ;; have an initial value to propagate.
                          (and (some? (:initial-value accumulator))
                               (empty? new-bindings)))

                :let [accum-result (do-accumulate accumulator filtered-facts)
                      converted-result (when (some? accum-result)
                                         (convert-return-fn accum-result))] 

                :when (some? converted-result)]

          (send-accumulated node accum-condition accumulator result-binding token
                            converted-result fact-bindings transport memory listener))

        ;; There are no previously accumulated results, but we still may need to propagate things
        ;; such as a sum of zero items.
        ;; If all variables in the accumulated item are bound and an initial
        ;; value is provided, we can propagate the initial value as the accumulated item.

        ;; We need to not propagate nil initial values, regardless of whether the convert-return-fn
        ;; makes them non-nil, in order to not break existing code; this is discussed more in the
        ;; right-activate-reduced implementation.
        (and (some? (:initial-value accumulator))
             (empty? new-bindings)) ; An initial value exists that we can propagate.
        (let [initial-value (:initial-value accumulator)
              ;; Note that we check the the :initial-value is non-nil above, which is why we
              ;; don't need (when initial-value (convert-return-fn initial-value)) here.
              converted-result (convert-return-fn initial-value)]

          (when (some? converted-result)
            ;; Send the created accumulated item to the children.
            (doseq [token tokens]
              (send-accumulated node accum-condition accumulator result-binding token
                                converted-result join-bindings transport memory listener))))

        ;; Propagate nothing if the above conditions don't apply.
        :default nil)))

  (left-retract [node join-bindings tokens memory transport listener]

    (let [;; Even if the accumulator didn't propagate anything before we still need to remove the tokens
          ;; in case they would have otherwise been used in the future.
          tokens (mem/remove-tokens! memory node join-bindings tokens)
          convert-return-fn (:convert-return-fn accumulator)
          grouped-candidate-facts (mem/get-accum-reduced-all memory node join-bindings)]

      (cond

        (seq grouped-candidate-facts)
        (doseq [token tokens
                [fact-bindings candidate-facts] grouped-candidate-facts

                :let [filtered-facts (filter-accum-facts join-filter-fn token candidate-facts)]

                :when (or (seq filtered-facts)
                          ;; Even if there no filtered facts, if there are no new bindings an initial value
                          ;; maybe have propagated, and if so we need to retract it.
                          (and (some? (:initial-value accumulator))
                               (empty? new-bindings)))

                :let [accum-result (do-accumulate accumulator filtered-facts)
                      retracted-converted (when (some? accum-result)
                                            (convert-return-fn accum-result))]

                ;; A nil retracted previous result should not have been propagated before.
                :when (some? retracted-converted)]

          (retract-accumulated node accum-condition accumulator result-binding token
                               retracted-converted fact-bindings transport memory listener))

        (and (some? (:initial-value accumulator))
             (empty? new-bindings))
        (let [initial-value (:initial-value accumulator)
              ;; Note that we check the the :initial-value is non-nil above, which is why we
              ;; don't need (when initial-value (convert-return-fn initial-value)) here.
              converted-result (convert-return-fn initial-value)]

          (when (some? converted-result)
            (doseq [token tokens]
              (retract-accumulated node accum-condition accumulator result-binding token
                                   converted-result join-bindings transport memory listener))))

        :else nil)))

  (get-join-keys [node] binding-keys)

  (description [node] (str "AccumulateWithBetaPredicateNode -- " accumulator))

  IAccumRightActivate
  (pre-reduce [node elements]
    ;; Return a map of bindings to the candidate facts that match them. This accumulator
    ;; depends on the values from parent facts, so we defer actually running the accumulator
    ;; until we have a token.
    (for [[bindings element-group] (platform/group-by-seq :bindings elements)]
      [bindings (map :fact element-group)]))

  (right-activate-reduced [node join-bindings binding-candidates-seq memory transport listener]

    ;; Combine previously reduced items together, join to matching tokens,
    ;; and emit child tokens.
    (doseq [:let [convert-return-fn (:convert-return-fn accumulator)
                  matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings candidates] binding-candidates-seq
            :let [previous-candidates (mem/get-accum-reduced memory node join-bindings bindings)
                  previously-reduced? (not= :clara.rules.memory/no-accum-reduced previous-candidates)
                  previous-candidates (when previously-reduced? previous-candidates)]]

      ;; Combine the newly reduced values with any previous items.  Ensure that new items are always added to the end so that
      ;; we have a consistent order for retracting results from accumulators such as acc/all whose results can be in any order.  Making this
      ;; ordering consistent allows us to skip the filter step on previous elements on right-activations.
      (let [combined-candidates (into []
                                      cat
                                      [previous-candidates candidates])]

        (l/add-accum-reduced! listener node join-bindings combined-candidates bindings)

        (mem/add-accum-reduced! memory node join-bindings combined-candidates bindings))
      
      (doseq [token matched-tokens

              :let [new-filtered-facts (filter-accum-facts join-filter-fn token candidates)]

              ;; If no new elements matched the token, we don't need to do anything for this token
              ;; since the final result is guaranteed to be the same.
              :when (seq new-filtered-facts)

              :let [previous-filtered-facts (filter-accum-facts join-filter-fn token previous-candidates)

                    previous-accum-result-init (cond
                                                 (seq previous-filtered-facts)
                                                 (do-accumulate accumulator previous-filtered-facts)

                                                 (and (-> accumulator :initial-value some?)
                                                      (empty? new-bindings))
                                                 (:initial-value accumulator)

                                                 ;; Allow direct determination later of whether there was a previous value
                                                 ;; as determined by the preceding cond conditions.
                                                 :else ::no-previous-value)

                    previous-accum-result (when (not= previous-accum-result-init ::no-previous-value)
                                            previous-accum-result-init)

                    ;; Since the new elements are added onto the end of the previous elements in the accum-memory
                    ;; accumulating using the new elements on top of the previous result is an accumulation in the same
                    ;; order as the elements are present in memory.  As a result, future accumulations on the contents of the accum memory
                    ;; prior to further modification of that memory will return the same result as here.  This is important since if we use
                    ;; something like acc/all to accumulate to and propagate [A B] if B is retracted we need to retract [A B] not [B A]; the latter won't
                    ;; actually retract anything, which would be invalid.
                    accum-result (let [accum-previous-init (if (not= previous-accum-result-init ::no-previous-value)
                                                             ;; If there was a previous result, use it as the initial value.
                                                             (assoc accumulator :initial-value previous-accum-result)
                                                             ;; If there was no previous result, use the default initial value.
                                                             ;; Note that if there is a non-nil initial value but there are new binding
                                                             ;; groups we consider there to have been no previous value, but we still want
                                                             ;; to use the actual initial value, not nil.
                                                             accumulator)]
                                   (do-accumulate accum-previous-init new-filtered-facts))
                    
                    previous-converted (when (some? previous-accum-result)
                                         (convert-return-fn previous-accum-result))
                    
                    new-converted (when (some? accum-result)
                                    (convert-return-fn accum-result))]]

        (cond

          ;; When both the new and previous result were nil do nothing.
          (and (nil? previous-converted)
               (nil? new-converted))
          nil

          (nil? new-converted)
          (retract-accumulated node accum-condition accumulator result-binding token
                               previous-converted bindings transport memory listener)

          (nil? previous-converted)
          (send-accumulated node accum-condition accumulator result-binding token new-converted bindings transport memory listener)

          (not= new-converted previous-converted)
          (do
            (retract-accumulated node accum-condition accumulator result-binding token
                                 previous-converted bindings transport memory listener)
            (send-accumulated node accum-condition accumulator result-binding token new-converted bindings transport memory listener))))))

  IRightActivate
  (right-activate [node join-bindings elements memory transport listener]

    ;; Simple right-activate implementation simple defers to
    ;; accumulator-specific logic.
    (right-activate-reduced
     node
     join-bindings
     (pre-reduce node elements)
     memory
     transport
     listener))

  (right-retract [node join-bindings elements memory transport listener]

    (doseq [:let [convert-return-fn (:convert-return-fn accumulator)
                  matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings elements] (platform/group-by-seq :bindings elements)
            :let [previous-candidates (mem/get-accum-reduced memory node join-bindings bindings)]

            ;; No need to retract anything if there was no previous item.
            :when (not= :clara.rules.memory/no-accum-reduced previous-candidates)

            :let [facts (mapv :fact elements)
                  new-candidates (second (mem/remove-first-of-each facts previous-candidates))]]
      
      ;; Add the new candidates to our node.
      (mem/add-accum-reduced! memory node join-bindings new-candidates bindings)

      (doseq [;; Get all of the previously matched tokens so we can retract and re-send them.
              token matched-tokens
              
              :let [previous-facts (filter-accum-facts join-filter-fn token previous-candidates)

                    new-facts (filter-accum-facts join-filter-fn token new-candidates)]

              ;; The previous matching elements are a superset of the matching elements after retraction.
              ;; Therefore, if the counts before and after are equal nothing retracted actually matched
              ;; and we don't need to do anything else here since the end result shouldn't change.
              :when (not= (count previous-facts)
                          (count new-facts))

              :let [;; We know from the check above that matching elements existed previously,
                    ;; since if there were no previous matching elements the count of matching
                    ;; elements before and after a right-retraction cannot be different.
                    previous-result (do-accumulate accumulator previous-facts)

                    ;; TODO: Can we use the retract-fn here if present to improve performance?  We'd also potentially
                    ;; avoid needing to filter facts twice above, since elements present both before and after retraction
                    ;; will given to the join-filter-fn twice (once when creating previous-facts and once when creating new-facts).
                    ;; Note that any future optimizations here must ensure that the result propagated here is equal to the result
                    ;; that will be recreated as the previous result in right activate, and that this can be dependent on the order
                    ;; of candidates in the memory, since, for example (acc/all) can return both [A B] and [B A] but these are not equal.

                    new-result (cond
                                 
                                 (seq new-facts)
                                 (do-accumulate accumulator new-facts)

                                 (and (-> accumulator :initial-value some?)
                                      (empty? new-bindings))
                                 (:initial-value accumulator)

                                 :else nil)
                    
                    previous-converted (when (some? previous-result)
                                         (convert-return-fn previous-result))
                    
                    new-converted (when (some? new-result)
                                    (convert-return-fn new-result))]]

        (cond
          
          ;; When both the previous and new results are nil do nothing.
          (and (nil? previous-converted)
               (nil? new-converted))
          nil

          (nil? new-converted)
          (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings transport memory listener)

          (nil? previous-converted)
          (send-accumulated node accum-condition accumulator result-binding token new-converted bindings transport memory listener)

          (not= previous-converted new-converted)
          (do
            (retract-accumulated node accum-condition accumulator result-binding token previous-converted bindings transport memory listener)
            (send-accumulated node accum-condition accumulator result-binding token new-converted bindings transport memory listener))))))) 

(defn variables-as-keywords
  "Returns symbols in the given s-expression that start with '?' as keywords"
  [expression]
  (into #{} (for [item (tree-seq coll? seq expression)
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
  [rulebase nodes transient-memory transport listener get-alphas-fn]
  (binding [*current-session* {:rulebase rulebase
                               :transient-memory transient-memory
                               :transport transport
                               :insertions (atom 0)
                               :get-alphas-fn get-alphas-fn
                               :pending-updates (atom [])
                               :listener listener}]

    (loop [next-group (mem/next-activation-group transient-memory)
           last-group nil]

      (if next-group

        (if (and last-group (not= last-group next-group))

          ;; We have changed groups, so flush the updates from the previous
          ;; group before continuing.
          (do
            (flush-updates *current-session*)
            (recur (mem/next-activation-group transient-memory) next-group))

          (do

            ;; If there are activations, fire them.
            (when-let [{:keys [node token]} (mem/pop-activation! transient-memory)]
              ;; Use vectors for the insertion caches so that within an insertion type
              ;; (unconditional or logical) all insertions are done in order after the into
              ;; calls in insert-facts!.  This shouldn't have a functional impact, since any ordering
              ;; should be valid, but makes traces less confusing to end users.
              (let [batched-logical-insertions (atom [])
                    batched-unconditional-insertions (atom [])
                    batched-rhs-retractions (atom [])]
                (binding [*rule-context* {:token token
                                          :node node
                                          :batched-logical-insertions batched-logical-insertions
                                          :batched-unconditional-insertions batched-unconditional-insertions
                                          :batched-rhs-retractions batched-rhs-retractions}]

                  ;; Fire the rule itself.
                  (try
                    ((:rhs node) token (:env (:production node)))
                    ;; Don't do anything if a given insertion type has no corresponding
                    ;; facts to avoid complicating traces.  Note that since each no RHS's of
                    ;; downstream rules are fired here everything is governed by truth maintenance.
                    ;; Therefore, the reordering of retractions and insertions should have no impact
                    ;; assuming that the evaluation of rule conditions is pure, which is a general expectation
                    ;; of the rules engine.
                    (when-let [batched (seq @batched-unconditional-insertions)]
                      (flush-insertions! batched true))
                    (when-let [batched (seq @batched-logical-insertions)]
                      (flush-insertions! batched false))
                    (when-let [batched (seq @batched-rhs-retractions)]
                      (flush-rhs-retractions! batched))
                    (catch #?(:clj Exception :cljs :default) e
                           
                           ;; If the rule fired an exception, help debugging by attaching
                           ;; details about the rule itself, cached insertions, and any listeners
                           ;; while propagating the cause.
                           (let [production (:production node)
                                 rule-name (:name production)
                                 rhs (:rhs production)]
                             (throw (ex-info (str "Exception in " (if rule-name rule-name (pr-str rhs))
                                                  " with bindings " (pr-str (:bindings token)))
                                             {:bindings (:bindings token)
                                              :name rule-name
                                              :rhs rhs
                                              :batched-logical-insertions @batched-logical-insertions
                                              :batched-unconditional-insertions @batched-unconditional-insertions
                                              :batched-rhs-retractions @batched-rhs-retractions
                                              :listeners (try
                                                           (let [p-listener (l/to-persistent! listener)]
                                                             (if (l/null-listener? p-listener)
                                                               []
                                                               (l/get-children p-listener)))
                                                           (catch #?(:clj Exception :cljs :default)
                                                             listener-exception
                                                             listener-exception))}
                                             e)))))

                  ;; Explicitly flush updates if we are in a no-loop rule, so the no-loop
                  ;; will be in context for child rules.
                  (when (some-> node :production :props :no-loop)
                    (flush-updates *current-session*)))))

            (recur (mem/next-activation-group transient-memory) next-group)))

        ;; There were no items to be activated, so flush any pending
        ;; updates and recur with a potential new activation group
        ;; since a flushed item may have triggered one.
        (when (flush-updates *current-session*)
          (recur (mem/next-activation-group transient-memory) next-group))))))


(deftype LocalSession [rulebase memory transport listener get-alphas-fn]
  ISession
  (insert [session facts]
    (let [transient-memory (mem/to-transient memory)
          transient-listener (l/to-transient listener)]

      (l/insert-facts! transient-listener facts)

      (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
              root alpha-roots]
        (alpha-activate root fact-group transient-memory transport transient-listener))

      (LocalSession. rulebase
                     (mem/to-persistent! transient-memory)
                     transport
                     (l/to-persistent! transient-listener)
                     get-alphas-fn)))

  (retract [session facts]

    (let [transient-memory (mem/to-transient memory)
          transient-listener (l/to-transient listener)]

      (l/retract-facts! transient-listener facts)

      (doseq [[alpha-roots fact-group] (get-alphas-fn facts)
              root alpha-roots]
        (alpha-retract root fact-group transient-memory transport transient-listener))

      (LocalSession. rulebase
                     (mem/to-persistent! transient-memory)
                     transport
                     (l/to-persistent! transient-listener)
                     get-alphas-fn)))

  (fire-rules [session]

    (let [transient-memory (mem/to-transient memory)
          transient-listener (l/to-transient listener)]
      (fire-rules* rulebase
                   (:production-nodes rulebase)
                   transient-memory
                   transport
                   transient-listener
                   get-alphas-fn)

      (LocalSession. rulebase
                     (mem/to-persistent! transient-memory)
                     transport
                     (l/to-persistent! transient-listener)
                     get-alphas-fn)))

  ;; TODO: queries shouldn't require the use of transient memory.
  (query [session query params]
    (let [query-node (get-in rulebase [:query-nodes query])]
      (when (= nil query-node)
        (platform/throw-error (str "The query " query " is invalid or not included in the rule base.")))

      (->> (mem/get-tokens (mem/to-transient memory) query-node params)

           ;; Get the bindings for each token and filter generate symbols.
           (map (fn [{bindings :bindings}]

                  ;; Filter generated symbols. We check first since this is an uncommon flow.
                  (if (some #(re-find #"__gen" (name %)) (keys bindings) )

                    (into {} (remove (fn [[k v]] (re-find #"__gen"  (name k)))
                                     bindings))
                    bindings))))))

  (components [session]
    {:rulebase rulebase
     :memory memory
     :transport transport
     :listeners (if (l/null-listener? listener)
                  []
                  (l/get-children listener))
     :get-alphas-fn get-alphas-fn}))

(defn assemble
  "Assembles a session from the given components, which must be a map
   containing the following:

   :rulebase A recorec matching the clara.rules.compiler/Rulebase structure.
   :memory An implementation of the clara.rules.memory/IMemoryReader protocol
   :transport An implementation of the clara.rules.engine/ITransport protocol
   :listeners A vector of listeners implementing the clara.rules.listener/IPersistentListener protocol
   :get-alphas-fn The function used to return the alpha nodes for a fact of the given type."

  [{:keys [rulebase memory transport listeners get-alphas-fn]}]
  (LocalSession. rulebase
                 memory
                 transport
                 (if (> (count listeners) 0)
                   (l/delegating-listener listeners)
                   l/default-listener)
                 get-alphas-fn))

(defn local-memory
  "Returns a local, in-process working memory."
  [rulebase transport activation-group-sort-fn activation-group-fn alphas-fn]
  (let [memory (mem/to-transient (mem/local-memory rulebase activation-group-sort-fn activation-group-fn alphas-fn))]
    (doseq [beta-node (:beta-roots rulebase)]
      (left-activate beta-node {} [empty-token] memory transport l/default-listener))
    (mem/to-persistent! memory)))

(defn options->activation-group-sort-fn
  "Given the map of options for a session, construct an activation group sorting
  function that takes into account the user-provided salience and internal salience.
  User-provided salience is considered first.  Under normal circumstances this function should
  only be called by Clara itself."
  [options]
  (let [user-activation-group-sort-fn (or (get options :activation-group-sort-fn)
                                          ;; Default to sort by descending numerical order.
                                          >)]

    ;; Compare user-provided salience first, using either the provided salience function or the default,
    ;; then use the internal salience if the former does not provide an ordering between the two salience values.
    (fn [salience1 salience2]
      (let [forward-result (user-activation-group-sort-fn (nth salience1 0)
                                                          (nth salience2 0))]
        (if (number? forward-result)
          (if (= 0 forward-result)
            (> (nth salience1 1)
               (nth salience2 1))
            forward-result)
          (let [backward-result (user-activation-group-sort-fn (nth salience2 0)
                                                               (nth salience1 0))
                forward-bool (boolean forward-result)
                backward-bool (boolean backward-result)]
            ;; Since we just use Clojure functions, for example >, equality may be implied
            ;; by returning false for comparisons in both directions rather than by returning 0.
            ;; Furthermore, ClojureScript will use truthiness semantics rather than requiring a
            ;; boolean (unlike Clojure), so we use the most permissive semantics between Clojure
            ;; and ClojureScript.
            (if (not= forward-bool backward-bool)
              forward-bool
              (> (nth salience1 1)
                 (nth salience2 1)))))))))

(def ^:private internal-salience-levels {:default 0
                                         ;; Extracted negations need to be prioritized over their original
                                         ;; rules since their original rule could fire before the extracted condition.
                                         ;; This is a problem if the original rule performs an unconditional insertion
                                         ;; or has other side effects not controlled by truth maintenance.
                                         :extracted-negation 1})

(defn options->activation-group-fn
  "Given a map of options for a session, construct a function that takes a production
  and returns the activation group to which it belongs, considering both user-provided
  and internal salience.  Under normal circumstances this function should only be called by
  Clara itself."
  [options]
  (let [rule-salience-fn (or (:activation-group-fn options)
                             (fn [production] (or (some-> production :props :salience)
                                                  0)))]

    (fn [production]
      [(rule-salience-fn production)
       (internal-salience-levels (or (some-> production :props :clara-rules/internal-salience)
                                     :default))])))

