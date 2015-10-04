(ns clara.test-common
  "Common tests for Clara in Clojure and ClojureScript."
  (:require #?(:clj  [clojure.test :refer :all]
               :cljs [cljs.test :refer-macros [is deftest]])

            #?(:clj  [clara.rules :refer :all]
               :cljs [clara.rules :refer [insert insert! fire-rules query]])

            [clara.rules.accumulators :as acc])

  ;; Load Clara's ClojureScript macros
  #?(:cljs (:require-macros [clara.macros :refer [defrule defsession defquery]])))

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

(defsession my-session 'clara.test-common :fact-type-fn :type)

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
                             :location "MCI"}))]

    ;; The query should identify all items that were inserted and matched the
    ;; expected criteria.
    (is (= #{{:?t 15} {:?t 10}}
           (set (query session cold-query))))))
