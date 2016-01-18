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


;; Simple, in-memory transport.
(deftype LocalTransport []
  ITransport
  (send-elements [transport memory listener nodes elements]

    (doseq [node nodes
            :let [join-keys (get-join-keys node)]]

      (if (> (count join-keys) 0)

        ;; Group by the join keys for the activation.
        (doseq [[join-bindings element-group] (platform/tuned-group-by #(select-keys (:bindings %) join-keys) elements)]
          (right-activate node
                          join-bindings
                          element-group
                          memory
                          transport
                          listener))

        ;; The node has no join keys, so just send everything at once
        ;; (if there is something to send.)
        (when (seq elements)
          (right-activate node
                          {}
                          elements
                          memory
                          transport
                          listener)))))

  (send-tokens [transport memory listener nodes tokens]

    (doseq [node nodes
            :let [join-keys (get-join-keys node)]]

      (if (> (count join-keys) 0)
        (doseq [[join-bindings token-group] (platform/tuned-group-by #(select-keys (:bindings %) join-keys) tokens)]

          (left-activate node
                         join-bindings
                         token-group
                         memory
                         transport
                         listener))

        ;; The node has no join keys, so just send everything at once.
        (when (seq tokens)
          (left-activate node
                         {}
                         tokens
                         memory
                         transport
                         listener)))))

  (retract-elements [transport memory listener nodes elements]
    (doseq  [[bindings element-group] (platform/tuned-group-by :bindings elements)
             node nodes]
      (right-retract node
                     (select-keys bindings (get-join-keys node))
                     element-group
                     memory
                     transport
                     listener)))

  (retract-tokens [transport memory listener nodes tokens]
    (doseq  [[bindings token-group] (platform/tuned-group-by :bindings tokens)
             node nodes]
      (left-retract  node
                     (select-keys bindings (get-join-keys node))
                     token-group
                     memory
                     transport
                     listener))))

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
  "Flush pending updates in the current session. Returns true if there were some items to flush,
  false otherwise"
  [current-session]

  (let [{:keys [rulebase transient-memory transport insertions get-alphas-fn listener]} current-session
        pending-updates @(:pending-updates current-session)]

    ;; Remove the facts here so they are re-inserted if we flush recursively.
    (reset! (:pending-updates current-session) [])

    (doseq [partition (partition-by :type pending-updates)
            :let [facts (mapcat :facts partition)]
            [alpha-roots fact-group] (get-alphas-fn facts)
            root alpha-roots]

      (if (= :insert (:type (first partition)))
        (alpha-activate root fact-group transient-memory transport listener)
        (alpha-retract root fact-group transient-memory transport listener)))

    (not (empty? pending-updates))))

(defn insert-facts!
  "Perform the actual fact insertion, optionally making them unconditional."
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
        (l/insert-facts-logical! listener node token facts)
        ))

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
    (let [token-insertion-map (mem/remove-insertions! memory node tokens)
          insertions (apply concat (vals token-insertion-map))]

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
            (alpha-retract root fact-group memory transport listener))))))

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

(defn- retract-accumulated
  "Helper function to retract an accumulated value."
  [node accum-condition accumulator result-binding token result fact-bindings transport memory listener]
  (let [converted-result ((:convert-return-fn accumulator) result)
        new-facts (conj (:matches token) [converted-result (:id node)])
        new-bindings (merge (:bindings token)
                            fact-bindings
                            (when result-binding
                              { result-binding
                                converted-result}))]

    (retract-tokens transport memory listener (:children node)
                    [(->Token new-facts new-bindings)])))

(defn- send-accumulated
  "Helper function to send the result of an accumulated value to the node's children."
  [node accum-condition accumulator result-binding token result fact-bindings transport memory listener]
  (let [converted-result ((:convert-return-fn accumulator) result)
        new-bindings (merge (:bindings token)
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

(defn- has-keys?
  "Returns true if the given map has all of the given keys."
  [m keys]
  (every? (partial contains? m) keys))

;; The AccumulateNode hosts Accumulators, a Rete extension described above, in the Rete network
;; It behavios similarly to a JoinNode, but performs an accumulation function on the incoming
;; working-memory elements before sending a new token to its descendents.
(defrecord AccumulateNode [id accum-condition accumulator result-binding children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    (let [previous-results (mem/get-accum-reduced-all memory node join-bindings)]
      (mem/add-tokens! memory node join-bindings tokens)

      (doseq [token tokens]

        (cond

         ;; If there are previously accumulated results to propagate, simply use them.
         (seq previous-results)
         (doseq [[fact-bindings previous] previous-results]
           (send-accumulated node accum-condition accumulator result-binding token previous fact-bindings transport memory listener))

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
           (send-accumulated node accum-condition accumulator result-binding token previous fact-bindings transport memory listener)

           (l/add-accum-reduced! listener node join-bindings previous fact-bindings)

           ;; Add it to the working memory.
           (mem/add-accum-reduced! memory node join-bindings previous fact-bindings))

         ;; Propagate nothing if the above conditions don't apply.
         :default nil))))

  (left-retract [node join-bindings tokens memory transport listener]
    (let [previous-results (mem/get-accum-reduced-all memory node join-bindings)]
      (doseq [token (mem/remove-tokens! memory node join-bindings tokens)
              [fact-bindings previous] previous-results]
        (retract-accumulated node accum-condition accumulator result-binding token previous fact-bindings transport memory listener))))

  (get-join-keys [node] binding-keys)

  (description [node] (str "AccumulateNode -- " accumulator))

  IAccumRightActivate
  (pre-reduce [node elements]
    ;; Return a map of bindings to the pre-reduced value.
    (for [[bindings element-group] (platform/tuned-group-by :bindings elements)]
      [bindings
       (r/reduce (:reduce-fn accumulator)
                 (:initial-value accumulator)
                 (r/map :fact element-group))]))

  (right-activate-reduced [node join-bindings reduced-seq  memory transport listener]
    ;; Combine previously reduced items together, join to matching tokens,
    ;; and emit child tokens.
    (doseq [:let [matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings reduced] reduced-seq
            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)]]

      (if (not= :clara.rules.memory/no-accum-reduced previous)

        ;; A previous value was reduced, so we need to retract it.
        (doseq [token matched-tokens]
          (retract-accumulated node accum-condition accumulator result-binding token
                               previous bindings transport memory listener))

        ;; The accumulator has an initial value that is effectively the previous result when
        ;; there was no previous item, so retract it.
        (when-let [initial-value (:initial-value accumulator)]
          (doseq [token matched-tokens]
            (retract-accumulated node accum-condition accumulator result-binding token initial-value
                                 {result-binding initial-value} transport memory listener))))

      ;; Combine the newly reduced values with any previous items.
      (let [combined (if (not= :clara.rules.memory/no-accum-reduced previous)
                       ((:combine-fn accumulator) previous reduced)
                       reduced)]

        (l/add-accum-reduced! listener node join-bindings combined bindings)

        (mem/add-accum-reduced! memory node join-bindings combined bindings)
        (doseq [token matched-tokens]
          (send-accumulated node accum-condition accumulator result-binding token combined bindings transport memory listener)))))

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

    (doseq [:let [matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings elements] (platform/tuned-group-by :bindings elements)
            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)]

            ;; No need to retract anything if there was no previous item.
            :when (not= :clara.rules.memory/no-accum-reduced previous)

            ;; Compute the new version with the retracted information.
            :let [facts (mapv :fact elements)
                  retracted (r/reduce (:retract-fn accumulator) previous facts)]]

      ;; Add our newly retracted information to our node.
      (mem/add-accum-reduced! memory node join-bindings retracted bindings)
      
      ;; Get all of the previously matched tokens so we can retract and re-send them.
      (doseq [token matched-tokens]

        ;; Retract the previous token.
        (retract-accumulated node accum-condition accumulator result-binding token previous bindings transport memory listener)

        ;; Send a new accumulated token with our new, retracted information.
        (when retracted
          (send-accumulated node accum-condition accumulator result-binding token retracted bindings transport memory listener))))))

(defn- do-accumulate
  "Runs the actual accumulation.  Returns the accumulated value if there are candidate facts
   that match the join filter or if there is an :initial-value to propagate from the accumulator.
   If neither of these conditions are true, ::no-accum-result is returned to indicate so.
   Note:  A returned value of nil is not the same as ::no-accum-result.  A nil value may be the result
          of running the accumulator over facts matching the token."
  [accumulator join-filter-fn token candidate-facts]
  (let [filtered-facts (filter #(join-filter-fn token % {}) candidate-facts)] ;; TODO: and env

    (if (or (:initial-value accumulator) (seq filtered-facts))
      (r/reduce (:reduce-fn accumulator)
                (:initial-value accumulator)
                filtered-facts)
      ::no-accum-result)))

;; A specialization of the AccumulateNode that supports additional tests
;; that have to occur on the beta side of the network. The key difference between this and the simple
;; accumulate node is the join-filter-fn, which accepts a token and a fact and filters out facts that
;; are not consistent with the given token.
(defrecord AccumulateWithJoinFilterNode [id accum-condition accumulator join-filter-fn
                                            result-binding children binding-keys]
  ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]

    ;; Facts that are candidates for matching the token are used in this accumulator node,
    ;; which must be filtered before running the accumulation.
    (let [grouped-candidate-facts (mem/get-accum-reduced-all memory node join-bindings)]
      (mem/add-tokens! memory node join-bindings tokens)

      (doseq [token tokens]

        (cond

         (seq grouped-candidate-facts)
         (doseq [[fact-bindings candidate-facts] grouped-candidate-facts

                 ;; Filter to items that match the incoming token, then apply the accumulator.
                 :let [accum-result (do-accumulate accumulator join-filter-fn token candidate-facts)]

                 ;; There is nothing to propagate if nothing was accumulated.
                 :when (not= ::no-accum-result accum-result)]

           (send-accumulated node accum-condition accumulator result-binding token accum-result fact-bindings transport memory listener))

         ;; There are no previously accumulated results, but we still may need to propagate things
         ;; such as a sum of zero items.
         ;; If all variables in the accumulated item are bound and an initial
         ;; value is provided, we can propagate the initial value as the accumulated item.

         (and (has-keys? (:bindings token)
                         binding-keys) ; All bindings are in place.
              (:initial-value accumulator)) ; An initial value exists that we can propagate.
         (let [fact-bindings (select-keys (:bindings token) binding-keys)
               initial-value (:initial-value accumulator)]

           ;; Send the created accumulated item to the children.
           (send-accumulated node accum-condition accumulator result-binding token initial-value fact-bindings transport memory listener)

           ;; This accumulator keeps candidate facts rather than fully reduced values in the working memory,
           ;; since the reduce operation must occur per token. Since there are no candidate facts
           ;; in this flow, just put an empty vector into our memory.
           (l/add-accum-reduced! listener node join-bindings [] fact-bindings)
           (mem/add-accum-reduced! memory node join-bindings [] fact-bindings))

         ;; Propagate nothing if the above conditions don't apply.
         :default nil))))

  (left-retract [node join-bindings tokens memory transport listener]

    (let [grouped-candidate-facts (mem/get-accum-reduced-all memory node join-bindings)]
      (doseq [token (mem/remove-tokens! memory node join-bindings tokens)
              [fact-bindings candidate-facts] grouped-candidate-facts

              :let [accum-result (do-accumulate accumulator join-filter-fn token candidate-facts)]

              ;; There is nothing to retract if nothing was accumulated.
              :when (not= ::no-accum-result accum-result)]

        (retract-accumulated node accum-condition accumulator result-binding token accum-result fact-bindings transport memory listener))))

  (get-join-keys [node] binding-keys)

  (description [node] (str "AccumulateWithBetaPredicateNode -- " accumulator))

  IAccumRightActivate
  (pre-reduce [node elements]
    ;; Return a map of bindings to the candidate facts that match them. This accumulator
    ;; depends on the values from parent facts, so we defer actually running the accumulator
    ;; until we have a token.
    (for [[bindings element-group] (platform/tuned-group-by :bindings elements)]
      [bindings (map :fact element-group)]))

  (right-activate-reduced [node join-bindings binding-candidates-seq memory transport listener]

    ;; Combine previously reduced items together, join to matching tokens,
    ;; and emit child tokens.
    (doseq [:let [matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings candidates] binding-candidates-seq
            :let [previous-candidates (mem/get-accum-reduced memory node join-bindings bindings)
                  previously-reduced? (not= :clara.rules.memory/no-accum-reduced previous-candidates)
                  previous-candidates (when previously-reduced? previous-candidates)]]

      (if previously-reduced?

       ;; Items were previously reduced that matched the bindings, so retract them to
       ;; allow the newly accumulated result to propagate.
       (doseq [token matched-tokens

               :let [previous-accum-result (do-accumulate accumulator join-filter-fn token previous-candidates)]

               ;; There is nothing to retract if nothing was accumulated.
               :when (not= ::no-accum-result previous-accum-result)]

         (retract-accumulated node accum-condition accumulator result-binding token
                              previous-accum-result bindings transport memory listener))

       ;; No items were previously reduced, but there may be an initial value that still needs to be
       ;; retracted.
       (when-let [initial-value (:initial-value accumulator)]

         (doseq [token matched-tokens]

           (retract-accumulated node accum-condition accumulator result-binding token initial-value
                                {result-binding initial-value} transport memory listener))))

      ;; Combine the newly reduced values with any previous items.
      (let [combined-candidates (into previous-candidates candidates)]

        (l/add-accum-reduced! listener node join-bindings combined-candidates bindings)

        (mem/add-accum-reduced! memory node join-bindings combined-candidates bindings)
        (doseq [token matched-tokens

                :let [accum-result (do-accumulate accumulator join-filter-fn token combined-candidates)]

                ;; There is nothing to propagate if nothing was accumulated.
                :when (not= ::no-accum-result accum-result)]

          (send-accumulated node accum-condition accumulator result-binding token accum-result bindings transport memory listener)))))

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

    (doseq [:let [matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings elements] (platform/tuned-group-by :bindings elements)
            :let [previous-candidates (mem/get-accum-reduced memory node join-bindings bindings)]

            ;; No need to retract anything if there was no previous item.
            :when (not= :clara.rules.memory/no-accum-reduced previous-candidates)

            :let [facts (mapv :fact elements)
                  new-candidates (second (mem/remove-first-of-each facts previous-candidates))]]
      
      ;; Add the new candidates to our node.
      (mem/add-accum-reduced! memory node join-bindings new-candidates bindings)

      (doseq [;; Get all of the previously matched tokens so we can retract and re-send them.
              token matched-tokens

              ;; Compute the new version with the retracted information.
              :let [previous-result (do-accumulate accumulator join-filter-fn token previous-candidates)
                    new-result (do-accumulate accumulator join-filter-fn token new-candidates)]]

        ;; Retract the previous token if something was previously accumulated.
        (when (not= ::no-accum-result previous-result)
          (retract-accumulated node accum-condition accumulator result-binding token previous-result bindings transport memory listener))

        ;; Send a new accumulated token with our new, retracted information when there is a new result.
        (when (and new-result (not= ::no-accum-result new-result))
          (send-accumulated node accum-condition accumulator result-binding token new-result bindings transport memory listener))))))

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

              (binding [*rule-context* {:token token :node node}]

                ;; Fire the rule itself.
                (try
                  ((:rhs node) token (:env (:production node)))
                  (catch #?(:clj Exception
                            :cljs :default) e

                         ;; If the rule fired an exception, help debugging by attaching
                         ;; details about the rule itself while propagating the cause.
                         (let [production (:production node)
                               rule-name (:name production)
                               rhs (:rhs production)]
                           (throw (ex-info (str "Exception in " (if rule-name rule-name (pr-str rhs))
                                                " with bindings " (pr-str (:bindings token)))
                                           {:bindings (:bindings token)
                                            :name rule-name
                                            :rhs rhs}
                                           e)))))

                ;; Explicitly flush updates if we are in a no-loop rule, so the no-loop
                ;; will be in context for child rules.
                (when (some-> node :production :props :no-loop)
                  (flush-updates *current-session*))))

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
