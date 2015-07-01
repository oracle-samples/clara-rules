(ns clara.rules.engine.nodes.accumulators
  "Rete network accumulator node types"
  (:require
    #?(:clj [clojure.core.reducers :as r])
    [clara.rules.platform :as platform]
    [clara.rules.engine.protocols :as impl]
    [clara.rules.engine.helpers :as hlp]
    [clara.rules.engine.wme :as wme]
    [clara.rules.memory :as mem] [clara.rules.listener :as l]))

(defn- send-accumulated
  "Helper function to send the result of an accumulated value to the node's children."
  [node accum-condition accumulator result-binding token result fact-bindings transport memory listener]
  (let [converted-result ((:convert-return-fn accumulator) result)
        new-bindings (merge (:bindings token)
                            fact-bindings
                            (when result-binding
                              { result-binding
                                converted-result}))]

    (impl/send-tokens transport memory listener (:children node)
                 [(wme/->Token (conj (:matches token) [converted-result (:id node)]) new-bindings)])))

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

    (impl/retract-tokens transport memory listener (:children node)
                    [(wme/->Token new-facts new-bindings)])))


;; The AccumulateNode hosts Accumulators, a Rete extension described above, in the Rete network
;; It behavios similarly to a JoinNode, but performs an accumulation function on the incoming
;; working-memory elements before sending a new token to its descendents.
(defrecord AccumulateNode [id accum-condition accumulator result-binding children binding-keys]
  impl/ILeftActivate
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

         (and (hlp/has-keys? (:bindings token)
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

  impl/IAccumRightActivate
  (pre-reduce [node elements]
    ;; Return a map of bindings to the pre-reduced value.
    (for [[bindings element-group] (platform/tuned-group-by :bindings elements)]
      [bindings
       #?(:clj
           (r/reduce (:reduce-fn accumulator)
                     (:initial-value accumulator)
                     (r/map :fact element-group))
           :cljs 
           (reduce (:reduce-fn accumulator)
                     (:initial-value accumulator)
                     (map :fact element-group)))]))

  (right-activate-reduced [node join-bindings reduced-seq  memory transport listener]
    ;; Combine previously reduced items together, join to matching tokens,
    ;; and emit child tokens.
    (doseq [:let [matched-tokens (mem/get-tokens memory node join-bindings)]
            [bindings reduced] reduced-seq
            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)]]

      (if (not= :clara.rules.memory/no-accum-reduced previous)

        ;; A previous value was reduced, so we need to retract it.
        (doseq [token (mem/get-tokens memory node join-bindings)]
          (retract-accumulated node accum-condition accumulator result-binding token
                               previous bindings transport memory listener))

        ;; The accumulator has an initial value that is effectively the previous result when
        ;; there was no previous item, so retract it.
        (when-let [initial-value (:initial-value accumulator)]
          (doseq [token (mem/get-tokens memory node join-bindings)]
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

  impl/IRightActivate
  (right-activate [node join-bindings elements memory transport listener]

    ;; Simple right-activate implementation simple defers to
    ;; accumulator-specific logic.
    (impl/right-activate-reduced
     node
     join-bindings
     (impl/pre-reduce node elements)
     memory
     transport
     listener))

  (right-retract [node join-bindings elements memory transport listener]

    (doseq [:let [matched-tokens (mem/get-tokens memory node join-bindings)]
            {:keys [fact bindings] :as element} elements
            :let [previous (mem/get-accum-reduced memory node join-bindings bindings)]

            ;; No need to retract anything if there was no previous item.
            :when (not= :clara.rules.memory/no-accum-reduced previous)

            ;; Get all of the previously matched tokens so we can retract and re-send them.
            token matched-tokens

            ;; Compute the new version with the retracted information.
            :let [retracted ((:retract-fn accumulator) previous fact)]]

      ;; Add our newly retracted information to our node.
      (mem/add-accum-reduced! memory node join-bindings retracted bindings)

      ;; Retract the previous token.
      (retract-accumulated node accum-condition accumulator result-binding token previous bindings transport memory listener)

      ;; Send a new accumulated token with our new, retracted information.
      (when retracted
        (send-accumulated node accum-condition accumulator result-binding token retracted bindings transport memory listener)))))

(defn- do-accumulate
  "Runs the actual accumulation.  Returns the accumulated value if there are candidate facts
   that match the join filter or if there is an :initial-value to propagate from the accumulator.
   If neither of these conditions are true, ::no-accum-result is returned to indicate so.
   Note:  A returned value of nil is not the same as ::no-accum-result.  A nil value may be the result
          of running the accumulator over facts matching the token."
  [accumulator join-filter-fn token candidate-facts]
  (let [filtered-facts (filter #(join-filter-fn token % {}) candidate-facts)] ;; TODO: and env

    (if (or (:initial-value accumulator) (seq filtered-facts))
      #?(:clj
          (r/reduce (:reduce-fn accumulator)
                   (:initial-value accumulator)
                   filtered-facts)
          :cljs
          (reduce (:reduce-fn accumulator)
                   (:initial-value accumulator)
                   filtered-facts))   
      ::no-accum-result)))

;; A specialization of the AccumulateNode that supports additional tests
;; that have to occur on the beta side of the network. The key difference between this and the simple
;; accumulate node is the join-filter-fn, which accepts a token and a fact and filters out facts that
;; are not consistent with the given token.
(defrecord AccumulateWithJoinFilterNode [id accum-condition accumulator join-filter-fn
                                            result-binding children binding-keys]
  impl/ILeftActivate
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

         (and (hlp/has-keys? (:bindings token)
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

  impl/IAccumRightActivate
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
       (doseq [token (mem/get-tokens memory node join-bindings)

               :let [previous-accum-result (do-accumulate accumulator join-filter-fn token previous-candidates)]

               ;; There is nothing to retract if nothing was accumulated.
               :when (not= ::no-accum-result previous-accum-result)]

         (retract-accumulated node accum-condition accumulator result-binding token
                              previous-accum-result bindings transport memory listener))

       ;; No items were previously reduced, but there may be an initial value that still needs to be
       ;; retracted.
       (when-let [initial-value (:initial-value accumulator)]

         (doseq [token (mem/get-tokens memory node join-bindings)]

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

  impl/IRightActivate
  (right-activate [node join-bindings elements memory transport listener]

    ;; Simple right-activate implementation simple defers to
    ;; accumulator-specific logic.
    (impl/right-activate-reduced
      node
      join-bindings
      (impl/pre-reduce node elements)
      memory
      transport
      listener))

  (right-retract [node join-bindings elements memory transport listener]
    (doseq [:let [matched-tokens (mem/get-tokens memory node join-bindings)]
            {:keys [fact bindings] :as element} elements
            :let [previous-candidates (mem/get-accum-reduced memory node join-bindings bindings)]

            ;; No need to retract anything if there was no previous item.
            :when (not= :clara.rules.memory/no-accum-reduced previous-candidates)

            ;; Get all of the previously matched tokens so we can retract and re-send them.
            token matched-tokens

            ;; Compute the new version with the retracted information.
            :let [previous-result (do-accumulate accumulator join-filter-fn token previous-candidates)
                  new-candidates (second (mem/remove-first-of-each [fact] previous-candidates))
                  new-result (do-accumulate accumulator join-filter-fn token new-candidates)]]

      ;; Add the new candidates to our node.
      (mem/add-accum-reduced! memory node join-bindings new-candidates bindings)

      ;; Retract the previous token if something was previously accumulated.
      (when (not= ::no-accum-result previous-result)
        (retract-accumulated node accum-condition accumulator result-binding token previous-result bindings transport memory listener))

      ;; Send a new accumulated token with our new, retracted information when there is a new result.
      (when (and new-result (not= ::no-accum-result new-result))
        (send-accumulated node accum-condition accumulator result-binding token new-result bindings transport memory listener)))))


