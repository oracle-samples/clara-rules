(ns clara.rules.engine.nodes
  "Rete network node types, accumulators are in a separate name space"
  (:require
    [clara.rules.engine.protocols :as impl]
    [clara.rules.engine.state :as state]
    [clara.rules.engine.helpers :as hlp]
    [clara.rules.engine.wme :as wme]
    [clara.rules.memory :as mem] [clara.rules.listener :as l]
    #?@(:clj [[schema.core :as sc] [clara.rules.engine.nodes.accumulators]])
    #?@(:cljs [[schema.core :as sc :include-macros true]
               [clara.rules.engine.nodes.accumulators :refer [AccumulateNode AccumulateWithJoinFilterNode]]]))
  #?(:clj (:import [clara.rules.engine.nodes.accumulators AccumulateNode AccumulateWithJoinFilterNode])))

(defn- retract-facts!
  "Perform the fact retraction."
  [facts]
  (swap! (:pending-updates state/*current-session*) into [(wme/->PendingUpdate :retract facts)]))

;; The test node represents a Rete extension in which
(defrecord TestNode [id test children]
  impl/ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    (impl/send-tokens
     transport
     memory
     listener
     children
     (filter test tokens)))

  (left-retract [node join-bindings tokens memory transport listener]
    (impl/retract-tokens transport memory listener children tokens))

  (get-join-keys [node] [])

  (description [node] (str "TestNode -- " (:text test))))

;; Record for the production node in the Rete network.
(defrecord ProductionNode [id production rhs]
  impl/ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]

    (l/left-activate! listener node tokens)

    ;; Fire the rule if it's not a no-loop rule, or if the rule is not
    ;; active in the current context.
    (when (or (not (get-in production [:props :no-loop]))
              (not (= production (get-in state/*rule-context* [:node :production]))))

      ;; Preserve tokens that fired for the rule so we
      ;; can perform retractions if they become false.
      (mem/add-tokens! memory node join-bindings tokens)

      (let [activations (for [token tokens]
                          (wme/->Activation node token))]

        (l/add-activations! listener node activations)

        ;; The production matched, so add the tokens to the activation list.
        (mem/add-activations! memory production activations))))

  (left-retract [node join-bindings tokens memory transport listener]

    (l/left-retract! listener node tokens)

    ;; Remove any tokens to avoid future rule execution on retracted items.
    (mem/remove-tokens! memory node join-bindings tokens)

    ;; Remove pending activations triggered by the retracted tokens.
    (let [activations (for [token tokens]
                        (wme/->Activation node token))]

      (l/remove-activations! listener node activations)
      (mem/remove-activations! memory production activations))

    ;; Retract any insertions that occurred due to the retracted token.
    (let [insertions (mem/remove-insertions! memory node tokens)]

      ;; If there is current session with rules firing, add these items to the queue
      ;; to be retracted so they occur in the same order as facts being inserted.
      (if state/*current-session*

        ;; Retract facts that have become untrue, unless they became untrue
        ;; because of an activation of the current rule that is :no-loop
        (when (or (not (get-in production [:props :no-loop]))
                  (not (= production (get-in state/*rule-context* [:node :production]))))

          (retract-facts! insertions))

        ;; The retraction is occuring outside of a rule-firing phase,
        ;; so simply retract them as an external caller would.
        (doseq [[cls fact-group] (group-by type insertions)
                root (get-in (mem/get-rulebase memory) [:alpha-roots cls])]
          (impl/alpha-retract root fact-group memory transport listener)))))

  (get-join-keys [node] [])

  (description [node] "ProductionNode"))


;; Record representing alpha nodes in the Rete network,
;; each of which evaluates a single condition and
;; propagates matches to its children.
(defrecord AlphaNode [env children activation]
  impl/IAlphaActivate
  (alpha-activate [node facts memory transport listener]
    (impl/send-elements
     transport
     memory
     listener
     children
     (for [fact facts
           :let [bindings (activation fact env)] :when bindings] ; FIXME: add env.
       (wme/->Element fact bindings))))

  (alpha-retract [node facts memory transport listener]

    (impl/retract-elements
     transport
     memory
     listener
     children
     (for [fact facts
           :let [bindings (activation fact env)] :when bindings] ; FIXME: add env.
       (wme/->Element fact bindings)))))

(defrecord RootJoinNode [id condition children binding-keys]
  impl/ILeftActivate
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

  impl/IRightActivate
  (right-activate [node join-bindings elements memory transport listener]

    (l/right-activate! listener node elements)

    ;; Add elements to the working memory to support analysis tools.
    (mem/add-elements! memory node join-bindings elements)
    ;; Simply create tokens and send it downstream.
    (impl/send-tokens
     transport
     memory
     listener
     children
     (for [{:keys [fact bindings] :as element} elements]
       (wme/->Token [[fact (:id node)]] bindings))))

  (right-retract [node join-bindings elements memory transport listener]

    (l/right-retract! listener node elements)

    ;; Remove matching elements and send the retraction downstream.
    (impl/retract-tokens
     transport
     memory
     listener
     children
     (for [{:keys [fact bindings] :as element} (mem/remove-elements! memory node join-bindings elements)]
       (wme/->Token [[fact (:id node)]] bindings)))))

;; Record for the join node, a type of beta node in the rete network. This node performs joins
;; between left and right activations, creating new tokens when joins match and sending them to
;; its descendents.
(defrecord JoinNode [id condition children binding-keys]
  impl/ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)
    (impl/send-tokens
     transport
     memory
     listener
     children
     (for [element (mem/get-elements memory node join-bindings)
           token tokens
           :let [fact (:fact element)
                 fact-binding (:bindings element)]]
       (wme/->Token (conj (:matches token) [fact id]) (conj fact-binding (:bindings token))))))

  (left-retract [node join-bindings tokens memory transport listener]
    (impl/retract-tokens
     transport
     memory
     listener
     children
     (for [token (mem/remove-tokens! memory node join-bindings tokens)
           element (mem/get-elements memory node join-bindings)
           :let [fact (:fact element)
                 fact-bindings (:bindings element)]]
       (wme/->Token (conj (:matches token) [fact id]) (conj fact-bindings (:bindings token))))))

  (get-join-keys [node] binding-keys)

  (description [node] (str "JoinNode -- " (:text condition)))

  impl/IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (mem/add-elements! memory node join-bindings elements)
    (impl/send-tokens
     transport
     memory
     listener
     children
     (for [token (mem/get-tokens memory node join-bindings)
           {:keys [fact bindings] :as element} elements]
       (wme/->Token (conj (:matches token) [fact id]) (conj (:bindings token) bindings)))))

  (right-retract [node join-bindings elements memory transport listener]
    (impl/retract-tokens
     transport
     memory
     listener
     children
     (for [{:keys [fact bindings] :as element} (mem/remove-elements! memory node join-bindings elements)
           token (mem/get-tokens memory node join-bindings)]
       (wme/->Token (conj (:matches token) [fact id]) (conj (:bindings token) bindings))))))

;; The NegationNode is a beta node in the Rete network that simply
;; negates the incoming tokens from its ancestors. It sends tokens
;; to its descendent only if the negated condition or join fails (is false).
(defrecord NegationNode [id condition children binding-keys]
  impl/ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)
    (when (empty? (mem/get-elements memory node join-bindings))
      (impl/send-tokens transport memory listener children tokens)))

  (left-retract [node join-bindings tokens memory transport listener]
    (mem/remove-tokens! memory node join-bindings tokens)
    (when (empty? (mem/get-elements memory node join-bindings))
      (impl/retract-tokens transport memory listener children tokens)))

  (get-join-keys [node] binding-keys)

  (description [node] (str "NegationNode -- " (:text condition)))

  impl/IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (mem/add-elements! memory node join-bindings elements)
    ;; Retract tokens that matched the activation, since they are no longer negatd.
    (impl/retract-tokens transport memory listener children (mem/get-tokens memory node join-bindings)))

  (right-retract [node join-bindings elements memory transport listener]
    (mem/remove-elements! memory node join-bindings elements)
    (when (empty? (mem/get-elements memory node join-bindings))
      (impl/send-tokens transport memory listener children (mem/get-tokens memory node join-bindings)))))


;; The QueryNode is a terminal node that stores the
;; state that can be queried by a rule user.
(defrecord QueryNode [id query param-keys]
  impl/ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    (l/left-activate! listener node tokens)
    (mem/add-tokens! memory node join-bindings tokens))

  (left-retract [node join-bindings tokens memory transport listener]
    (l/left-retract! listener node tokens)
    (mem/remove-tokens! memory node join-bindings tokens))

  (get-join-keys [node] param-keys)

  (description [node] (str "QueryNode -- " query)))

;; A specialization of the NegationNode that supports additional tests
;; that have to occur on the beta side of the network. The key difference between this and the simple
;; negation node is the join-filter-fn, which allows negation tests to
;; be applied with the parent token in context, rather than just a simple test of the non-existence
;; on the alpha side.
(defrecord NegationWithJoinFilterNode [id condition join-filter-fn children binding-keys]
  impl/ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener]
    ;; Add token to the node's working memory for future right activations.
    (mem/add-tokens! memory node join-bindings tokens)

    (impl/send-tokens transport
                 memory
                 listener
                 children
                 (for [token tokens
                       :when (not (hlp/matches-some-facts? token
                                                       (mem/get-elements memory node join-bindings)
                                                       join-filter-fn
                                                       condition))]
                   token)))

  (left-retract [node join-bindings tokens memory transport listener]
    (mem/remove-tokens! memory node join-bindings tokens)
    (impl/retract-tokens transport
                    memory
                    listener
                    children

                    ;; Retract only if it previously had no matches in the negation node,
                    ;; and therefore had an activation.
                    (for [token tokens
                          :when (not (hlp/matches-some-facts? token
                                                          (mem/get-elements memory node join-bindings)
                                                          join-filter-fn
                                                          condition))]
                      token)))

  (get-join-keys [node] binding-keys)

  (description [node] (str "NegationWithJoinFilterNode -- " (:text condition)))

  impl/IRightActivate
  (right-activate [node join-bindings elements memory transport listener]
    (mem/add-elements! memory node join-bindings elements)
    ;; Retract tokens that matched the activation, since they are no longer negated.
    (impl/retract-tokens transport
                    memory
                    listener
                    children
                    (for [token (mem/get-tokens memory node join-bindings)

                          :when (hlp/matches-some-facts? token
                                                     elements
                                                     join-filter-fn
                                                     condition)]
                      token)))

  (right-retract [node join-bindings elements memory transport listener]
    (mem/remove-elements! memory node join-bindings elements)

    (impl/send-tokens transport
                 memory
                 listener
                 children
                 (for [token (mem/get-tokens memory node join-bindings)

                       ;; Propagate tokens when some of the retracted facts joined
                       ;; but none of the remaining facts do.
                       :when (and (hlp/matches-some-facts? token
                                                     elements
                                                     join-filter-fn
                                                     condition)
                                  (not (hlp/matches-some-facts? token
                                                            (mem/get-elements memory node join-bindings)
                                                            join-filter-fn
                                                            condition)))]
                   token))))


;; These nodes exist in the beta network.
(def BetaNode (sc/either ProductionNode QueryNode JoinNode RootJoinNode
                         NegationNode NegationWithJoinFilterNode
                         TestNode AccumulateNode AccumulateWithJoinFilterNode))


