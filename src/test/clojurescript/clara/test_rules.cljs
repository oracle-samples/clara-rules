(ns clara.test-rules
  (:require-macros [cljs.test :refer (is deftest run-tests testing)]
                   [clara.test-rules-data])
  (:require [cljs.test :as t]
            [clara.rules.engine :as eng]
            [clara.rules.accumulators :as acc]
            [clara.rules :refer [insert retract fire-rules query insert!]
                         :refer-macros [defrule defsession defquery]]
            [clara.rules.testfacts :refer [->Temperature Temperature
                                           ->WindSpeed WindSpeed
                                           ->ColdAndWindy ColdAndWindy]]))

(comment
;; Launch browser repl.
 (cemerick.piggieback/cljs-repl :repl-env (cemerick.austin/exec-env))
)

(defn- has-fact? [token fact]
  (some #{fact} (map first (:matches token))))

(def simple-defrule-side-effect (atom nil))
(def other-defrule-side-effect (atom nil))

(defrule test-rule
  [Temperature (< temperature 20)]
  =>
  (reset! other-defrule-side-effect ?__token__)
  (reset! simple-defrule-side-effect ?__token__))

(defquery cold-query
  []
  [Temperature (< temperature 20) (== ?t temperature)])

;; Accumulator for getting the lowest temperature.
(def lowest-temp (acc/min :temperature))

(defquery coldest-query
  []
  [?t <- lowest-temp :from [Temperature]])


(defrule is-cold-and-windy
  "Rule to determine whether it is indeed cold and windy."

  (Temperature (< temperature 20) (== ?t temperature))
  (WindSpeed (> windspeed 30) (== ?w windspeed))
  =>
  (insert! (->ColdAndWindy ?t ?w)))

(defrule is-cold-and-windy-map
  "A rule which uses a custom type on a map, to determine whether it
  is indeed cold and windy"

  [:temp [{degrees :degrees}] (< degrees 20) (== ?t degrees)]
  [:wind [{mph :mph}] (> mph 30) (== ?w mph)]
  =>
  (insert! {:type :cold-and-windy
            :temp ?t
            :wind ?w}))

(defrule throw-on-bad-temp
  "Rule to test exception flow."
  [Temperature (> temperature 10000) (= ?t temperature)]
  =>
  (throw (ex-info "Bad temperature!" {:temp ?t})))

(defquery find-cold-and-windy
    []
    [?fact <- ColdAndWindy])

(defquery find-cold-and-windy-map
    []
    [?fact <- :cold-and-windy])

(defquery wind-without-temperature
  []
  [WindSpeed (== ?w windspeed)]
  [:not [Temperature]])

(defquery wind-with-temperature
  []
  [WindSpeed (== ?w windspeed) (== ?loc location)]
  [Temperature (== ?t temperature) (== ?loc location)])

(defsession my-session 'clara.test-rules)
(defsession my-session-map 'clara.test-rules :fact-type-fn :type)
(defsession my-session-data (clara.test-rules-data/weather-rules))


(deftest test-simple-defrule
  (let [session (insert my-session (->Temperature 10 "MCI"))]

    (fire-rules session)

    (is (has-fact? @simple-defrule-side-effect (->Temperature 10 "MCI")))
    (is (has-fact? @other-defrule-side-effect (->Temperature 10 "MCI")))))

(deftest test-simple-query
  (let [session (-> my-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    ;; The query should identify all items that wer einserted and matchd the
    ;; expected criteria.
    (is (= #{{:?t 15} {:?t 10}}
           (set (query session cold-query))))))

(deftest test-simple-accumulator
  (let [session (-> my-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->Temperature 10 "MCI"))
                    (insert (->Temperature 80 "MCI")))]

    ;; Accumulator returns the lowest value.
    (is (= #{{:?t 10}}
           (set (query session coldest-query))))))

(deftest test-simple-insert
  (let [session (-> my-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->WindSpeed 45 "MCI"))
                    (fire-rules))]

    (is (= #{{:?fact (->ColdAndWindy 15 45)}}
           (set
            (query session find-cold-and-windy))))))

(deftest test-simple-insert-map

  (let [session (-> my-session-map
                    (insert {:type :temp :degrees 15})
                    (insert {:type :wind :mph 45})
                    (fire-rules))]
    (is (= #{{:?fact {:type :cold-and-windy :temp 15 :wind 45}}}
           (set
            (query session find-cold-and-windy-map))))))

(deftest test-simple-insert-data

  (let [session (-> my-session-data
                    (insert (->Temperature 15 "MCI"))
                    (insert (->WindSpeed 45 "MCI"))
                    (fire-rules))]
    (is (= #{{:?fact (->ColdAndWindy 15 45)}}
           (set
            (query session "clara.test-rules-data/find-cold-and-windy-data"))))))

(deftest test-no-temperature

  ;; Test that a temperature cancels the match.
  (let [session (-> my-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->WindSpeed 45 "MCI"))
                    (fire-rules))]

    (is (= #{}
           (set
            (query session wind-without-temperature)))))

  ;; Now test the no temperature scenario.
  (let [session (-> my-session
                    (insert (->WindSpeed 45 "MCI"))
                    (fire-rules))]

    (is (= #{{:?w 45}}
           (set
            (query session wind-without-temperature))))))


(deftest test-simple-join

  (let [session (-> my-session
                    (insert (->Temperature 15 "MCI"))
                    (insert (->WindSpeed 45 "MCI"))
                    (fire-rules))]

    (is (= #{{:?w 45 :?t 15 :?loc "MCI"}}
           (set
            (query session wind-with-temperature))))))

(deftest test-throw-rhs

  (try
    (-> my-session
        (insert (->Temperature 999999 "MCI"))
        (fire-rules))
    (catch :default e

      (is (= {:?t 999999}
             (:bindings (ex-data e))))
      (is (= "clara.test-rules/throw-on-bad-temp"
             (:name (ex-data e)))))))

(deftest test-remove-pending-rule-activation
  (let [no-activations-session (-> my-session
                                   (insert (->Temperature -10 "ORD")
                                           (->WindSpeed 50 "ORD"))
                                   (retract (->WindSpeed 50 "ORD"))
                                   fire-rules)

        one-activation-session (-> my-session
                                   (insert (->Temperature -10 "ORD")
                                           (->WindSpeed 50 "ORD")
                                           (->WindSpeed 50 "ORD"))
                                   (retract (->WindSpeed 50 "ORD"))
                                   fire-rules)]

    (is (= (query no-activations-session find-cold-and-windy) []))
    (is (= (query one-activation-session find-cold-and-windy)
           [{:?fact (->ColdAndWindy -10 50)}]))))                   
