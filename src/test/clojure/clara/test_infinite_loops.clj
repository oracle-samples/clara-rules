(ns clara.test-infinite-loops
  (:require [clojure.test :refer :all]
            [clara.rules :refer :all]
            [clara.rules.testfacts :refer [->Cold  ->Hot ->First ->Second]]
            [clara.tools.testing-utils :refer [def-rules-test
                                               ex-data-maps
                                               side-effect-holder-fixture
                                               side-effect-holder
                                               assert-ex-data]]
            [clara.tools.tracing :as tr]
            [clara.rules.accumulators :as acc]
            [clara.tools.loop-detector :as ld])
  (:import [clara.rules.testfacts Cold Hot First Second]
           [clara.tools.tracing
            PersistentTracingListener]))

(use-fixtures :each side-effect-holder-fixture)

(def-rules-test test-standard-out-warning

  {:rules [cold-rule [[[?cold-count <- (acc/count) :from [Cold]]]
                      (when (< ?cold-count 40)
                        (insert-unconditional! (->Cold "ORD")))]]

   :sessions [empty-session [cold-rule] {}]}

  ;; Test that both the keyword standard out function and a user-provided function can
  ;; be used.
  (doseq [limit-fn [:standard-out-warning (fn [] (println "an infinite loop is suspected"))]
          :let [warn-regex #"an infinite loop is suspected"]]

    ;; We test both that the warning is printed, and that it is only printed once - spamming the user
    ;; on each activation group change after the warning is reached would be unhelpful.
    (is (= (count
            (re-seq warn-regex (with-out-str (fire-rules (ld/with-loop-detection empty-session 10 limit-fn)))))
           1))

    ;; Test that we don't fire until the limit is reached; in this case the restriction in the rule RHS will stop it from
    ;; firing before the limit is reached.
    (is (= (count
            (re-seq warn-regex (with-out-str (fire-rules (ld/with-loop-detection empty-session 1000 limit-fn)))))
           0))))

(def-rules-test test-truth-maintenance-loop

  ;; Test of an infinite loop to an endless cycle caused by truth maintenance,
  ;; that is an insertion that causes its support to be retracted.

  {:rules [hot-rule [[[:not [Hot]]]
                     (insert! (->Cold nil))]

           cold-rule [[[Cold]]
                      (insert! (->Hot nil))]]

   :sessions [empty-session [hot-rule cold-rule] {}]}

  (let [watched-session (ld/with-loop-detection empty-session 3000 :throw-exception)]

    (assert-ex-data  {:clara-rules/infinite-loop-suspected true}  (fire-rules watched-session))))

(def-rules-test test-truth-maintenance-loop-with-salience

  ;; Test of an infinite loop to an endless cycle caused by truth maintenance,
  ;; that is an insertion that causes its support to be retracted, when the
  ;; two rules that form the loop have salience and are thus parts of different
  ;; activation groups.

  
  {:rules [hot-rule [[[:not [Hot]]]
                     (insert! (->Cold nil))
                     {:salience 1}]

           cold-rule [[[Cold]]
                      (insert! (->Hot nil))
                      {:salience 2}]]

   :sessions [empty-session [hot-rule cold-rule] {}
              empty-session-negated-salience [hot-rule cold-rule] {:activation-group-fn (comp - :salience :props)}]}

  (doseq [session (map #(ld/with-loop-detection % 3000 :throw-exception)
                       [empty-session empty-session-negated-salience])]
    ;; Validate that the results are the same in either rule ordering.

    (assert-ex-data  {:clara-rules/infinite-loop-suspected true}  (fire-rules session))))


(def-rules-test test-recursive-insertion

  ;; Test of an infinite loop due to runaway insertions without retractions.

  {:rules [cold-rule [[[Cold]]
                      (insert! (->Cold "ORD"))]]

   :sessions [empty-session [cold-rule] {}]}

  (let [watched-session (ld/with-loop-detection empty-session 10 :throw-exception)]

    (assert-ex-data {:clara-rules/infinite-loop-suspected true}
                    (-> watched-session
                        (insert (->Cold "ARN"))
                        ;; Use a small value here to ensure that we don't run out of memory before throwing.
                        ;; There is unfortunately a tradeoff where making the default number of cycles allowed
                        ;; high enough for some use cases will allow others to create OutOfMemory errors in cases
                        ;; like these but we can at least throw when there is enough memory available to hold the facts
                        ;; created in the loop.
                        (fire-rules {:max-cycles 10})))))

(def-rules-test test-recursive-insertion-loop-no-salience

  {:rules [first-rule [[[First]]
                       (insert! (->Second))]

           second-rule [[[Second]]
                        (insert! (->First))]]

   :sessions [empty-session [first-rule second-rule] {}]}

  (let [watched-session (ld/with-loop-detection empty-session 10 :throw-exception)]

    (assert-ex-data {:clara-rules/infinite-loop-suspected true}
                    (-> watched-session
                        (insert (->First))
                        (fire-rules {:max-cycles 10})))))

(def-rules-test test-recursive-insertion-loop-with-salience

  {:rules [first-rule [[[First]]
                       (insert! (->Second))
                       {:salience 1}]

           second-rule [[[Second]]
                        (insert! (->First))
                        {:salience 2}]]

   :sessions [empty-session [first-rule second-rule] {}
              empty-session-negated-salience [first-rule second-rule] {:activation-group-fn (comp - :salience :props)}]}

  (doseq [session
          (map #(ld/with-loop-detection % 10 :throw-exception)
               [empty-session empty-session-negated-salience])]
    (assert-ex-data {:clara-rules/infinite-loop-suspected true}
                    (-> session
                        (insert (->First))
                        (fire-rules {:max-cycles 10})))))

(def-rules-test test-tracing-infinite-loop

  {:rules [cold-rule [[[Cold]]
                      (insert! (->Cold "ORD"))]]

   :sessions [empty-session [cold-rule] {}]}

  (let [watched-session (ld/with-loop-detection empty-session 10 :throw-exception)]

    (try
      (do (-> watched-session
              tr/with-tracing
              (insert (->Cold "ARN"))
              ;; Use a small value here to ensure that we don't run out of memory before throwing.
              ;; There is unfortunately a tradeoff where making the default number of cycles allowed
              ;; high enough for some use cases will allow others to create OutOfMemory errors in cases
              ;; like these but we can at least throw when there is enough memory available to hold the facts
              ;; created in the loop.
              fire-rules)
          (is false "The infinite loop did not throw an exception."))
      (catch Exception e
        (let [data-maps (ex-data-maps e)
              loop-data (filter :clara-rules/infinite-loop-suspected data-maps)]
          (is (= (count loop-data)
                 1)
              "There should only be one exception in the chain from infinite rules loops.")
          (is (-> loop-data first :trace)
              "There should be tracing data available when a traced session throws an exception on an infinite loop."))))))

(def-rules-test test-max-cycles-respected

  ;; As the name suggests, this is a test to validate that setting the max-cycles to different
  ;; values actually works.  The others tests are focused on validating that infinite loops
  ;; throw exceptions, and the max-cycles values there are simply set to restrict resource usage,
  ;; whether CPU or memory, by the test.  Here, on the other hand, we construct a rule where we control
  ;; how many rule cycles will be executed, and then validate that an exception is thrown when that number is
  ;; greater than the max-cycles and is not when it isn't.  For good measure, we validate that the non-exception-throwing
  ;; case has queryable output to make sure that the logic was actually run, not ommitted because of a typo or similar.

  {:rules [recursive-rule-with-end [[[First]]
                                    (when (< @side-effect-holder 20)
                                      (do
                                        (insert-unconditional! (->First))
                                        (swap! side-effect-holder inc)))]]

   :queries [first-query [[] [[?f <- First]]]]

   :sessions [empty-session [recursive-rule-with-end first-query] {}]}

  (reset! side-effect-holder 0)

  (assert-ex-data {:clara-rules/infinite-loop-suspected true}
                  (-> (ld/with-loop-detection empty-session 10 :throw-exception)
                      (insert (->First))
                      (fire-rules {:max-cycles 10})))

  (reset! side-effect-holder 0)

  (is (= (count (-> (ld/with-loop-detection empty-session 30 :throw-exception)
                    (insert (->First))
                    (fire-rules {:max-cycles 30})
                    (query first-query)))
         21)))
