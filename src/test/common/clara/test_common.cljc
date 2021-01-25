(ns clara.test-common
  "Common tests for Clara in Clojure and ClojureScript."
  (:require #?(:clj  [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [is deftest testing]])

            #?(:clj  [clara.rules :refer :all]
               :cljs [clara.rules :refer [insert insert! fire-rules query]
                                  :refer-macros [defrule defsession defquery]])

            [clara.rules.accumulators :as acc]

            [clara.rules.platform :as platform]))

(defn- has-fact? [token fact]
  (some #{fact} (map first (:matches token))))

(def simple-defrule-side-effect (atom nil))
(def other-defrule-side-effect (atom nil))

(defrule test-rule
  [:temperature [{temperature :temperature}] (< temperature 20)]
  =>
  (reset! other-defrule-side-effect ?__token__)
  (reset! simple-defrule-side-effect ?__token__))

(defquery cold-query
  []
  [:temperature [{temperature :temperature}] (< temperature 20) (= ?t temperature)])

(defsession my-session [test-rule cold-query] :fact-type-fn :type)

(deftest test-simple-defrule
  (let [t {:type :temperature
           :temperature 10
           :location "MCI"}
        session (insert my-session t)]

    (fire-rules session)

    (is (has-fact? @simple-defrule-side-effect t))
    (is (has-fact? @other-defrule-side-effect t))))

(deftest test-simple-query
  (let [session (-> my-session
                    (insert {:type :temperature
                             :temperature 15
                             :location "MCI"})
                    (insert {:type :temperature
                             :temperature 10
                             :location "MCI"})
                    (insert {:type :temperature
                             :temperature 80
                             :location "MCI"})
                    fire-rules)]

    ;; The query should identify all items that were inserted and matched the
    ;; expected criteria.
    (is (= #{{:?t 15} {:?t 10}}
           (set (query session cold-query))))))


(defquery temps-below-threshold
  []
  [:threshold [{value :value}] (= ?threshold value)]
  [?low-temps <- (acc/all) :from [:temperature [{value :value}] (< value ?threshold)]])

(defsession accum-with-filter-session [temps-below-threshold] :fact-type-fn :type)

(deftest test-accum-with-filter

  (is (= [{:?threshold 0, :?low-temps []}]
       (-> accum-with-filter-session
           (insert {:type :temperature :value 20})
           (insert {:type :threshold :value 0})
           (insert {:type :temperature :value 10})
           fire-rules
           (query temps-below-threshold))))

  (let [results (-> accum-with-filter-session
                    (insert {:type :temperature :value 20})
                    (insert {:type :threshold :value 40})
                    (insert {:type :temperature :value 10})
                    (insert {:type :temperature :value 60})
                    fire-rules
                    (query temps-below-threshold))

        [{threshold :?threshold low-temps :?low-temps }] results]

    (is (= 1 (count results)))

    (is (= 40 threshold))

    (is (= #{{:type :temperature :value 10} {:type :temperature :value 20}}
           (set low-temps)))))

(defquery none-below-threshold
  []
  [:threshold [{value :value}] (= ?threshold value)]
  [:not [:temperature [{value :value}] (< value ?threshold)]])

(defquery temperature-below-value-using-symbol-arg
  [?value]
  [:temperature [{value :value}] (< value ?value)])

(defquery temperature-below-value-using-keyword-arg
  [:?value]
  [:temperature [{value :value}] (< value ?value)])

(deftest test-query-definition-bindings-args
  (testing "can define queries using symbol or keyword arguments"
    (is (= (dissoc temperature-below-value-using-symbol-arg :name) (dissoc temperature-below-value-using-keyword-arg :name)))))

(deftest test-query-param-args
  (testing "noop query-params using keyword arguments"
    (is (= (platform/query-param :?value) :?value)))
  (testing "coerce query-params using symbol arguments"
    (is (= (platform/query-param '?value) :?value)))
  (testing "can not coerce query-params using string arguments"
    (try
      (platform/query-param "?value")
      (is false "Running the rules in this test should cause an exception.")
      (catch #?(:clj java.lang.IllegalArgumentException
                :cljs js/Error) e
        (is (= "Query bindings must be specified as a keyword or symbol: ?value"
               #?(:clj (.getMessage e)
                  :cljs  (.-message e))))))))

(defsession negation-with-filter-session [none-below-threshold] :fact-type-fn :type)

(deftest test-negation-with-filter

  (is (= [{:?threshold 0}]
         (-> negation-with-filter-session
             (insert {:type :temperature :value 20})
             (insert {:type :threshold :value 0})
             (insert {:type :temperature :value 10})
             fire-rules
             (query none-below-threshold))))

  ;; Values below the threshold exist, so we should not match.
  (is (empty?
       (-> negation-with-filter-session
           (insert {:type :temperature :value 20})
           (insert {:type :threshold :value 40})
           (insert {:type :temperature :value 10})
           (insert {:type :temperature :value 60})
           fire-rules
           (query none-below-threshold)))))
