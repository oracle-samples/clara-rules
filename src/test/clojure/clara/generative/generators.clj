(ns clara.generative.generators
  (:require [clojure.math.combinatorics :as combo]
            [clara.rules :refer :all]
            [schema.core :as s]))

(s/defschema FactSessionOperation {:type (s/enum :insert :retract)
                                    :facts [s/Any]})

(s/defschema FireSessionOperation {:type (s/enum :fire)})

(s/defschema SessionOperation (s/conditional
                                #(= (:type %) :fire) FireSessionOperation
                                :else FactSessionOperation))

(defn session-run-ops
  "Run the provided sequence of operations on the provide session and return the final session."
  [session ops]
  (let [final-session (reduce (fn [session op]
                                (let [new-session (condp = (:type op)
                                                    :insert (insert-all session (:facts op))
                                                    :retract (apply retract session (:facts op))
                                                    :fire (fire-rules session))]
                                  new-session))
                              session ops)]
    final-session))

(defn ^:private retract-before-insertion?
  "Given a sequence of operations, determine if the number of retractions of any fact exceeds the number
  of times has been inserted at any time."
  [ops]
  (let [alter-fact-count (fn [alter-fn]
                           (fn [fc facts]
                             (reduce (fn [updated-fc f]
                                       (update fc f alter-fn))
                                     fc facts)))
        inc-fact-count (alter-fact-count (fnil inc 0))
        dec-fact-count (alter-fact-count (fnil dec 0))

        any-count-negative? (fn [fc]
                              (boolean (some neg? (vals fc))))]
    
    (= ::premature-retract (reduce (fn [fact-count op]
                                     (let [new-count (condp = (:type op)
                                                       :insert (inc-fact-count fact-count (:facts op))
                                                       :retract (dec-fact-count fact-count (:facts op))
                                                       fact-count)]
                                       (if (any-count-negative? new-count)
                                         (reduced ::premature-retract)
                                         new-count)))
                                   {}
                                   ops))))

(defn ^:private ops->add-insert-retract
  "Takes an operations sequence and returns a sequence of sequences of operations
  where every possible combination of inserting and retracting each fact inserted
  in the parent sequence up to the number of times set by dup-level.  Note that one
  of the possibilities returned will be the original operations sequence.  This is not
  special-cased, but a reflection of the fact that every combination of adding insert/retract pairs
  from 0 to dup-level is in the possibilities returned.  The number of possibilities returned
  will explode rapidly for large dup-level values."
  [ops dup-level]
  (let [ops->extra (fn [ops]
                     (map (fn [op]
                            (when (= (:type op)
                                     :insert)
                              [{:type :insert
                                :facts (:facts op)}
                               {:type :retract
                                :facts (:facts op)}]))
                          ops))
        extras-with-dups (apply concat (repeat dup-level
                                               (ops->extra ops)))

        extra-subsets (combo/subsets extras-with-dups)]

    (map (fn [extras]
           (into ops cat extras))
         extra-subsets)))

(s/defn ops->permutations :- [[SessionOperation]]
  "Given a sequence of operations, return all permutations of the operations for 
   which no retraction of a fact that has not yet been inserted occurs.  By default 
   permutations where extra insertions and retractions in equal number of inserted facts
   are added will be present.  The default number of such pairs allowed to be added per insertion
   is 1."
  [ops :- [SessionOperation]
   {:keys [dup-level] :or {dup-level 0}}]
  (let [dup-ops-seqs (ops->add-insert-retract ops dup-level)
        permutations (mapcat combo/permutations dup-ops-seqs)]
    
    ;; The permutation creation allows for a retraction to occur before insertion, which
    ;; effectively removes the retraction from the seq of operations since retractions of facts
    ;; that are not present do not cause alteration of the session state.  The idea of these helpers
    ;; is to produce orders of operations that should all have the same outcomes.
    ;; The following would have an A when done:
    ;; retract A, insert A
    ;; while this would not:
    ;; insert A, retract A
    ;;
    ;; For now, we can just find all permutations and remove the ones with invalid ordering.
    ;; This is inefficient and there may be a more efficient algorithm or implementation.
    (remove retract-before-insertion? permutations)))             
