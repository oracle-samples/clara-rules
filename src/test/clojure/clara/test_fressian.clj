(ns clara.test-fressian
  (:require [clara.rules.durability :as d]
            [clara.rules.durability.fressian :as df]
            [clojure.data.fressian :as fres]
            [clara.rules.platform :as pform]
            [clojure.test :refer :all])
  (:import [org.fressian
            FressianWriter
            FressianReader]))

(defn custom-comparator [x y]
  (> y x))

(defrecord Tester [x])

(defn serde1 [x]
  (with-open [os (java.io.ByteArrayOutputStream.)
              ^FressianWriter wtr (fres/create-writer os :handlers df/write-handler-lookup)]
    ;; Write
    (pform/thread-local-binding [d/node-id->node-cache (volatile! {})
                                 d/clj-record-holder (java.util.IdentityHashMap.)]
                                (fres/write-object wtr x))
    ;; Read
    (let [data (.toByteArray os)]
      (pform/thread-local-binding [d/clj-record-holder (java.util.ArrayList.)]
                                  (with-open [is (java.io.ByteArrayInputStream. data)
                                              ^FressianReader rdr (fres/create-reader is :handlers df/read-handler-lookup)]
                                    (fres/read-object rdr))))))

(defn serde [x]
  ;; Tests all serialization cases in a way that SerDe's 2 times to show that the serialization to
  ;; deserialization process does not lose important details for the next time serializing it.
  (-> x serde1 serde1))

(defn test-serde [expected x]
  (is (= expected (serde x))))

(defn test-serde-with-meta [expected x]
  (let [no-meta (serde x)
        test-meta {:test :meta}
        x-with-meta (vary-meta x merge test-meta)
        ;; In case x already has metadata it needs to be added to the expectation
        ;; along with the test metadata added in case it has none to test already.
        expected-meta (meta x-with-meta)
        has-meta (serde x-with-meta)]

    (is (= expected
           no-meta
           has-meta))
    (is (= expected-meta
           (meta has-meta)))))

(deftest test-handlers

  (testing "class"
    (test-serde String String))

  (testing "set"
    (test-serde-with-meta #{:x :y} #{:x :y}))
  
  (testing "vec"
    (test-serde-with-meta [1 2 3] [1 2 3]))

  (testing "list"
    (test-serde-with-meta (list "a" "b") (list "a" "b")))

  (testing "aseq"
    (test-serde-with-meta ['a 'b] (seq ['a 'b])))

  (testing "lazy seq"
    (test-serde-with-meta [2 3 4] (map inc [1 2 3])))

  (testing "map"
    (test-serde-with-meta {:x 1 :y 2} {:x 1 :y 2}))

  (testing "map entry"
    (let [e (first {:x 1})]
      (test-serde [:x 1] e)
      (is (instance? clojure.lang.MapEntry (serde e))
          "preserves map entry type to be sure to still work with `key` and `val`")))
  
  (testing "sym"
    (test-serde-with-meta 't 't))

  (testing "record"
    (test-serde-with-meta (->Tester 10) (->Tester 10)))
  
  (testing "sorted collections"
    (let [ss (sorted-set 1 10)
          ss-custom (with-meta (sorted-set-by custom-comparator 1 10)
                      {:clara.rules.durability/comparator-name `custom-comparator})

          sm (sorted-map 1 :x 10 :y)
          sm-custom (with-meta (sorted-map-by custom-comparator 1 :x 10 :y)
                      {:clara.rules.durability/comparator-name `custom-comparator})]

      (testing "set"
        (test-serde-with-meta ss ss)
        (test-serde-with-meta ss-custom ss-custom)
        (is (thrown? Exception
                     (serde (with-meta ss-custom {})))
            "cannot serialized custom sort comparators without name given in metadata"))

      (testing "map"
        (test-serde-with-meta sm sm)
        (test-serde-with-meta sm-custom sm-custom)
        (is (thrown? Exception
                     (serde (with-meta sm-custom {})))
            "cannot serialized custom sort comparators without name given in metadata")))))

