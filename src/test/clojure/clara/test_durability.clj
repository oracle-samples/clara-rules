(ns clara.test-durability
  (:require [clara.rules :refer :all]
            [clara.rules.dsl :as dsl]
            [clara.rules.engine :as eng]
            [clara.rules.durability :as d]
            [clara.rules.durability.fressian :as df]
            [clara.durability-rules :as dr]
            [clara.rules.accumulators :as acc]
            [clara.rules.testfacts :refer :all]
            [schema.test :as st]
            [clojure.java.io :as jio]
            [clojure.test :refer :all]
            [clara.rules.compiler :as com]
            [clara.tools.testing-utils :as tu])
  (:import [clara.rules.testfacts Temperature]
           [clara.rules.engine TestNode]))
           

(use-fixtures :once st/validate-schemas)

(defrecord LocalMemorySerializer [holder]
  d/IWorkingMemorySerializer
  (serialize-facts [_ fact-seq]
    (reset! holder fact-seq))
  (deserialize-facts [_]
    @holder))

(defn check-fact
  "Helper for checking facts in durability-test."
  [expected-fact fact]

  ;; Test equality first.  The tests need to be stricter than this, but if this isn't true nothing
  ;; else will be.  This will give the best failure messages first too in the more obvious non-equal
  ;; cases.
  (when (is (= expected-fact fact)
            "The expected and actual must be equal")
    
    (or (identical? expected-fact fact)
        (and (is (= (coll? expected-fact)
                    (coll? fact))
                 (str "not identical? with expected should mean both are collections" \newline
                      "expected fact: " expected-fact \newline
                      "fact: " fact \newline))

             (is (= (count expected-fact)
                    (count fact))
                 (str "not identical? with expected should mean both are the"
                      " same size collections" \newline
                      "expected fact: " expected-fact \newline
                      "fact: " fact \newline))

             ;; Not handling collection types not being used right now in the test.
             ;; This is only using the rules from clara.durability-rules.
             ;; If more sophisticated aggregates are used in join bindings, fact bindings,
             ;; or accumulated results in these tests case, the testing here will have to
             ;; be made more robust to show it.
             (cond
               (sequential? expected-fact)
               (and (is (sequential? fact)
                        (str "expected is sequential?" \newline
                             "expected fact: " expected-fact \newline
                             "fact: " fact \newline))
                    (mapv check-fact
                          expected-fact
                          fact))

               (set? expected-fact)
               (and (is (set? fact)
                        "expected is set?")
                    (every? #(is (identical? (expected-fact %) %)
                                 (str "the fact from one set must be found in the"
                                      " expected set and be identical? to it" \newline
                                      "expected fact: " expected-fact \newline
                                      "fact: " fact \newline))
                            fact))

               :else
               (is false
                   (str "Must find a matching comparison with the expected."
                        "  Most of the time this means the facts should be identical?" \newline
                        "expected fact: " expected-fact \newline
                        "fact: " fact \newline)))))))

(defn session-test [s]
  (let [mci "MCI"
        lax "LAX"
        san "SAN"
        chi "CHI"
        irk "IRK"
        ten 10
        twenty 20 
        fifty 50
        forty 40
        thirty 30
        thresh50 (dr/->Threshold fifty)
        temp50 (->Temperature fifty mci)
        temp40 (->Temperature forty lax)
        temp30 (->Temperature thirty san)
        temp20 (->Temperature twenty chi)
        ws50 (->WindSpeed fifty mci)
        ws40 (->WindSpeed forty lax)
        ws10 (->WindSpeed ten irk)
        fired (-> s
                  (insert thresh50
                          temp50
                          temp40
                          temp30
                          temp20
                          ws50
                          ws40
                          ws10)
                  fire-rules)

        unpaired-res (query fired dr/unpaired-wind-speed)
        cold-res (query fired dr/cold-temp)
        hot-res (query fired dr/hot-temp)
        temp-his-res (query fired dr/temp-his)
        temps-under-thresh-res (query fired dr/temps-under-thresh)]
    {:all-objs [mci
                lax
                san
                chi
                irk
                ten
                twenty
                fifty
                forty
                thirty
                thresh50
                temp50
                temp40
                temp30
                temp20
                ws50
                ws40
                ws10]
     :fired-session fired
     :query-results {:unpaired-res unpaired-res
                     :cold-res cold-res
                     :hot-res hot-res
                     :temp-his-res temp-his-res
                     :temps-under-thresh-res temps-under-thresh-res}}))

(defn durability-test
  "Test runner to run different implementations of d/ISessionSerializer."
  [serde-type]
  (let [s (mk-session 'clara.durability-rules)
        results (session-test s)
        ;; Testing identity relationships on the IWorkingMemorySerializer facts received to serialize.
        ;; So this is a little weird, but we want to know the exact object identity of even these
        ;; "primitive" values.
        [mci
         lax
         san
         chi
         irk
         ten
         twenty
         fifty
         forty
         thirty
         thresh50
         temp50
         temp40
         temp30
         temp20
         ws50
         ws40
         ws10] (:all-objs results)
        
        fired (:fired-session results)

        {:keys [unpaired-res
                cold-res
                hot-res
                temp-his-res 
                temps-under-thresh-res]} (:query-results results)

        create-serializer (fn [stream]
                            ;; Currently only one.
                            (condp = serde-type
                              :fressian (df/create-session-serializer stream)))

        rulebase-baos (java.io.ByteArrayOutputStream.)
        rulebase-serializer (create-serializer rulebase-baos)

        session-baos (java.io.ByteArrayOutputStream.)
        session-serializer (create-serializer session-baos)

        holder (atom [])
        mem-serializer (->LocalMemorySerializer holder)]

    ;; Serialize the data.  Store the rulebase seperately.  This is likely to be the most common usage.
    
    (d/serialize-rulebase fired
                          rulebase-serializer)
    (d/serialize-session-state fired
                               session-serializer
                               mem-serializer)

    (let [rulebase-data (.toByteArray rulebase-baos)
          session-data (.toByteArray session-baos)

          rulebase-bais (java.io.ByteArrayInputStream. rulebase-data)
          session-bais (java.io.ByteArrayInputStream. session-data)
          rulebase-serializer (create-serializer rulebase-bais)
          session-serializer (create-serializer session-bais)

          restored-rulebase (d/deserialize-rulebase rulebase-serializer)
          restored (d/deserialize-session-state session-serializer
                                                mem-serializer
                                                {:base-rulebase restored-rulebase})
          
          r-unpaired-res (query restored dr/unpaired-wind-speed)
          r-cold-res (query restored dr/cold-temp)
          r-hot-res (query restored dr/hot-temp)
          r-temp-his-res (query restored dr/temp-his)
          r-temps-under-thresh-res (query restored dr/temps-under-thresh)

          facts @(:holder mem-serializer)]

      (testing "Ensure the queries return same before and after serialization"
        (is (= (frequencies [{:?ws (dr/->UnpairedWindSpeed ws10)}])
               (frequencies unpaired-res)
               (frequencies r-unpaired-res)))

        (is (= (frequencies [{:?c (->Cold 20)}])
               (frequencies cold-res)
               (frequencies r-cold-res)))

        (is (= (frequencies [{:?h (->Hot 50)}
                             {:?h (->Hot 40)}
                             {:?h (->Hot 30)}])
               (frequencies hot-res)
               (frequencies r-hot-res)))

        (is (= (frequencies [{:?his (->TemperatureHistory [50 40 30 20])}])
               (frequencies temp-his-res)
               (frequencies r-temp-his-res)))

        (is (= (frequencies [{:?tut (dr/->TempsUnderThreshold [temp40 temp30 temp20])}])
               (frequencies temps-under-thresh-res)
               (frequencies r-temps-under-thresh-res))))

      (testing "metadata is preserved on rulebase nodes"
        (let [node-with-meta (->> s
                                  eng/components
                                  :rulebase
                                  :id-to-node
                                  vals
                                  (filter #(meta %))
                                  first)
              restored-node-with-meta (-> restored-rulebase
                                          :id-to-node
                                          (get (:id node-with-meta)))]
          (is (= (meta node-with-meta) (meta restored-node-with-meta)))))

      (testing (str "facts given to serialize-facts of IWorkingMemorySerializer"
                    " from ISessionSerializer have identity relationships"
                    " retained and accumulated values present.")
        ;; Unfortunately what seems like the best way to test this right now is to just manually
        ;; write out the whole expectation.  This is brittle, but hopefully doesn't change
        ;; that often.
        (let [cold20 (-> cold-res first :?c)
              unpaired-ws10 (-> unpaired-res first :?ws)
              temp-his (-> temp-his-res first :?his)
              temps-under-thresh (-> temps-under-thresh-res first :?tut)
              [hot30 hot40 hot50] (->> hot-res (map :?h) (sort-by :temperature))

              ;; All of these facts must have an identical? relationship (same object references)
              ;; as the actual facts being tested against.
              expected-facts [temp50
                              temp40
                              temp30
                              temp20
                              [temp50 temp40 temp30 temp20]
                              mci
                              lax
                              san
                              chi
                              twenty
                              cold20
                              unpaired-ws10
                              temp-his
                              ws50
                              ws40
                              ws10
                              irk
                              fifty
                              forty
                              thirty
                              thresh50
                              temps-under-thresh
                              hot40
                              hot30
                              hot50
                              [temp40 temp30 temp20]]]
          
          (is (= (count expected-facts)
                 (count facts))
              (str "expected facts:" \newline
                   (vec expected-facts) \newline
                   "actual facts:" \newline
                   (vec facts)))
          
          (doseq [i (range (count expected-facts))
                  :let [expected-fact (nth expected-facts i)
                        fact (nth facts i)]]
            (check-fact expected-fact fact)))))))

(defn rb-serde
  [s deserialize-opts]
  (with-open [baos (java.io.ByteArrayOutputStream.)]
    (d/serialize-rulebase s (df/create-session-serializer baos))
    (let [rb-data (.toByteArray baos)]
      (with-open [bais (java.io.ByteArrayInputStream. rb-data)]
        (if deserialize-opts
          (d/deserialize-rulebase (df/create-session-serializer bais) deserialize-opts)
          (d/deserialize-rulebase (df/create-session-serializer bais)))))))
 
(deftest test-durability-fressian-serde
  (testing "SerDe of the rulebase along with working memory"
    (durability-test :fressian))

  (testing "Repeated SerDe of rulebase"
    (let [s (mk-session 'clara.durability-rules)
          rb (-> s eng/components :rulebase)
          deserialized1 (rb-serde s nil)
          ;; Need a session to do the 2nd round of SerDe.
          restored1 (d/assemble-restored-session deserialized1 {})
          deserialized2 (rb-serde restored1 nil)
          restored2 (d/assemble-restored-session deserialized2 {})

          init-qresults (:query-results (session-test s))
          restored-qresults1 (:query-results (session-test restored1))
          restored-qresults2 (:query-results (session-test restored2))]
      
      (is (= init-qresults
             restored-qresults1
             restored-qresults2)))))

(defn get-test-nodes
  [session]
  (->> session
       eng/components
       :rulebase
       :id-to-node
       vals
       (filter (partial instance? TestNode))))

(deftest test-durability-testnode-serde
  (let [s (mk-session 'clara.durability-rules)
        rb (-> s eng/components :rulebase)
        deserialized1 (rb-serde s nil)
        ;; Need a session to do the 2nd round of SerDe.
        restored1 (d/assemble-restored-session deserialized1 {})
        deserialized2 (rb-serde restored1 nil)
        restored2 (d/assemble-restored-session deserialized2 {})]
    (testing "ensure that a TestNode's ICondition/get-condition-description implementation survives serialization and deserialization"
      (is (= [[:test '(not-empty ?ts)]]
             (map eng/get-condition-description (get-test-nodes s))
             (map eng/get-condition-description (get-test-nodes restored1))
             (map eng/get-condition-description (get-test-nodes restored2)))))
    (testing "ensure that a TestNode which has gone through SerDe still works correctly by inserting a new fact, re-firing the rules, and then querying to ensure the test node worked"
      (let [{:keys [fired-session]} (session-test restored2)]
        (is (= [{:?his (->TemperatureHistory [50 40 30 20 10])}]
               (-> fired-session
                   (insert (->Temperature 10 "ORD"))
                   (fire-rules)
                   (query dr/temp-his))))))))

(deftest test-assemble-restored-session-opts
  (let [orig (mk-session 'clara.durability-rules)

        test-assemble (fn [orig assemble-with-memory?]
                        (let [memory (-> orig eng/components :memory)
                              activation-group-fn-called? (volatile! false)
                              activation-group-sort-fn-called? (volatile! false)
                              fact-type-fn-called? (volatile! false)
                              ancestors-fn-called? (volatile! false)
                              
                              opts {:activation-group-fn (fn [x]
                                                           (vreset! activation-group-fn-called? true)
                                                           (or (some-> x :props :salience)
                                                               0))
                                    :activation-group-sort-fn (fn [x y]
                                                                (vreset! activation-group-sort-fn-called? true)
                                                                (> x y))
                                    :fact-type-fn (fn [x]
                                                    (vreset! fact-type-fn-called? true)
                                                    (type x))
                                    :ancestors-fn (fn [x]
                                                    (vreset! ancestors-fn-called? true)
                                                    (ancestors x))}

                              rulebase (rb-serde orig opts)
                              
                              restored (if assemble-with-memory?
                                         (d/assemble-restored-session rulebase memory opts)
                                         (d/assemble-restored-session rulebase opts))]

                          (is (= (:query-results (session-test orig))
                                 (:query-results (session-test restored))))

                          (is (true? @activation-group-sort-fn-called?))
                          (is (true? @activation-group-fn-called?))
                          (is (true? @fact-type-fn-called?))
                          (is (true? @ancestors-fn-called?))))]

    (testing "restoring without given memory"
      (test-assemble orig false))

    (testing "restoring with memory"
      (test-assemble orig true))))

;; Issue 422 (https://github.com/cerner/clara-rules/issues/422)
;; A test to demonstrate the difference in error messages provided when compilation context is omitted
(deftest test-compilation-ctx
  (def test-compilation-ctx-var 123)
  (let [rule (dsl/parse-rule [[Long (== this test-compilation-ctx-var)]]
                             (println "here"))
        without-compile-ctx (com/mk-session [[rule]])
        with-compile-ctx (com/mk-session [[rule] :omit-compile-ctx false])]
    ;; Simulate deserializing in an environment without this var by unmapping it.
    (ns-unmap 'clara.test-durability 'test-compilation-ctx-var)
    (try
      (rb-serde without-compile-ctx nil)
      (is false "Error not thrown when deserializing the rulebase without ctx")
      (catch Exception e
        ;; In the event that the compilation context is not retained the original condition of the node will not be present.
        (is (some #(and (not (contains? % :condition))
                        ;; Validate that the exception is related to the Clara compilation failure (and so the check above is valid).
                        (:expr %))
                  (tu/get-all-ex-data e)))))

    (try
      (rb-serde with-compile-ctx nil)
      (is false "Error not thrown when deserializing the rulebase with ctx")
      (catch Exception e

        (is (some #(= (select-keys (:condition %) [:type :constraints])
                      {:type  Long
                       :constraints ['(== this clara.test-durability/test-compilation-ctx-var)]})
                  (tu/get-all-ex-data e)))))))
