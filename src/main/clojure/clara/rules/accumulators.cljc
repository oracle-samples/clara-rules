(ns clara.rules.accumulators
  "A set of common accumulators usable in Clara rules."
  (:require [clara.rules.engine :as eng]
            [clojure.set :as set]
            [schema.core :as s])
  (:refer-clojure :exclude [min max distinct count]))


(defn accum
  "Creates a new accumulator. Users are encouraged to use a pre-defined
   accumulator in this namespace if one fits their needs. (See min, max, all,
   distinct, and others in this namespace.) This function
   exists for cases where a custom accumulator is necessary.

   The following properties are accepted.

   * An initial-value to be used with the reduced operations.
   * A reduce-fn that can be used with the Clojure Reducers library to reduce items.
   * A combine-fn that can be used with the Clojure Reducers library to combine reduced items.
   * A retract-fn that can remove a retracted fact from a previously reduced computation.
   * An optional convert-return-fn that converts the reduced data into something useful to the caller.
     Simply uses identity by default.
    "
  [{:keys [initial-value reduce-fn combine-fn retract-fn convert-return-fn] :as accum-map}]

  ;; Validate expected arguments are present.
  (s/validate {(s/optional-key :initial-value) s/Any
               (s/optional-key :combine-fn) s/Any
               (s/optional-key :convert-return-fn) s/Any
               :reduce-fn s/Any
               :retract-fn s/Any}
              accum-map)

  (eng/map->Accumulator
   (merge
    {:combine-fn reduce-fn ; Default combine function is simply the reduce.
     :convert-return-fn identity ; Default conversion does nothing, so use identity.
     }
    accum-map)))

(defn- drop-one-of
  "Removes one instance of the given value from the sequence."
  [items value]
  (let [pred #(not= value %)]
    (into (take-while pred items)
          (rest (drop-while pred items)))))


(defn reduce-to-accum
  "Creates an accumulator using a given reduce function with optional initial value and
   conversion to the final result.

   For example, a a simple function that return a Temperature fact with the highest value:

     (acc/reduce-to-accum
        (fn [previous value]
           (if previous
              (if (> (:temperature value) (:temperature previous))
                 value
                 previous)
              value)))

   Note that the above example produces the same result as
   (clara.rules.accumulators/max :temperature :returns-fact true),
   and users should prefer to use built-in accumulators when possible. This funciton exists to easily
   convert arbitrary reduce functions to an accumulator.

   Callers may optionally pass in an initial value (which defaults to nil),
   a function to transform the value returned by the reduce (which defaults to identity),
   and a function to combine two reduced results (which uses the reduce-fn by default)."

  ([reduce-fn]
   (reduce-to-accum reduce-fn nil))
  ([reduce-fn initial-value]
   (reduce-to-accum reduce-fn initial-value identity))
  ([reduce-fn initial-value convert-return-fn]
   (reduce-to-accum reduce-fn initial-value convert-return-fn reduce-fn))
  ([reduce-fn initial-value convert-return-fn combine-fn]

   (let [wrapped-initial-value (if (nil? initial-value)
                                  nil
                                  [[] initial-value])

         wrapped-reduce-fn (fn [[seen reduced] item]
                              [(conj (or seen []) item) (reduce-fn reduced item)])

         wrapped-combine-fn (let [wrapper-combine-fn (or combine-fn reduce-fn)]
                               (fn [[seen1 reduced1] [seen2 reduced2]]
                                 [(concat seen1 seen2) (wrapper-combine-fn reduced1 reduced2)]))

         wrapped-retract-fn (fn [[seen _] retracted]
                               (reduce wrapped-reduce-fn
                                       wrapped-initial-value
                                       (let [[left right] (split-with (partial not= retracted) seen)]
                                         (concat left (rest right)))))

         wrapped-convert-return-fn (if (nil? convert-return-fn)
                                      (fn [[_ reduced]]
                                        reduced)
                                      (fn [[_ reduced]]
                                        (convert-return-fn reduced)))]

     (accum
      {:initial-value wrapped-initial-value
       :reduce-fn wrapped-reduce-fn
       :retract-fn wrapped-retract-fn
       :combine-fn wrapped-combine-fn
       :convert-return-fn wrapped-convert-return-fn}))))

(defn- comparison-based
  "Creates a comparison-based result such as min or max"
  [field comparator returns-fact supports-retract]
  (let [reduce-fn (fn [previous value]
                    (if previous
                      (if (comparator (field previous) (field value))
                        previous
                        value)
                      value))

        convert-return-fn (if returns-fact
                            identity
                            field)]

    (if supports-retract
      (reduce-to-accum reduce-fn nil convert-return-fn)

      ;; No need to support retraction, so we can
      ;; create a more efficient accumulator directly.
      (accum
       {:reduce-fn reduce-fn
        ;; Retract does nothing.
        :retract-fn (fn [value item] value)
        :convert-return-fn convert-return-fn}))))


(defn min
  "Returns an accumulator that returns the minimum value of a given field.

   The caller may provide the following options:

   * :returns-fact Returns the fact rather than the field value if set to true. Defaults to false.
   * :supports-retract Keeps the history of facts used so the minimum value can be updated if
     the previous fact with the minimum value is retracted. This guarantees the expected
     behavior in the face of retractions but comes at the cost of maintaining all matching facts."

  [field & {:keys [returns-fact supports-retract] :or {supports-retract true}}]
  (comparison-based field < returns-fact supports-retract))

(defn max
  "Returns an accumulator that returns the maximum value of a given field.

   The caller may provide the following options:

   * :returns-fact Returns the fact rather than the field value if set to true. Defaults to false.
   * :supports-retract Keeps the history of facts used so the minimum value can be updated if
     the previous fact with the minimum value is retracted. This guarantees the expected
     behavior in the face of retractions but comes at the cost of maintaining all matching facts."

  [field & {:keys [returns-fact supports-retract] :or {supports-retract true}}]
  (comparison-based field > returns-fact supports-retract))

(defn average
  "Returns an accumulator that returns the average value of a given field."
  [field]
  (accum
   {:initial-value [0 0]
    :reduce-fn (fn [[value count] item]
                 [(+ value (field item)) (inc count)])
    :retract-fn (fn [[value count] retracted]
                  [(- value (field retracted)) (dec count)])
    :combine-fn (fn [[value1 count1] [value2 count2]]
                  [(+ value1 value2) (+ count1 count2)])
    :convert-return-fn (fn [[value count]]
                         (if (= 0 count)
                           nil
                           (/ value count)))}))

(defn sum
  "Returns an accumulator that returns the sum of values of a given field"
  [field]
  (accum
   {:initial-value 0
    :reduce-fn (fn [total item]
                 (+ total (field item)))
    :retract-fn (fn [total item]
                  (- total (field item)))
    :combine-fn +}))

(defn count
  "Returns an accumulator that simply counts the number of matching facts"
  []
  (accum
   {:initial-value 0
    :reduce-fn (fn [count value] (inc count))
    :retract-fn (fn [count retracted] (dec count))
    :combine-fn +}))

(defn distinct
  "Returns an accumulator producing a distinct set of facts.
   If given a field, returns a distinct set of values for that field."
  ([]
     (accum
      {:initial-value #{}
       :reduce-fn (fn [items value] (conj items value))
       :retract-fn (fn [items retracted] (disj items retracted))
       :combine-fn set/union}))
  ([field]
     (accum
      {:initial-value #{}
       :reduce-fn (fn [items value] (conj items (field value)))
       :retract-fn (fn [items retracted] (disj items (field retracted)))
       :combine-fn set/union})))

(defn all
  "Returns an accumulator that preserves all accumulated items.
   If given a field, returns all values in that field."
  ([]
     (accum
      {:initial-value []
       :reduce-fn (fn [items value] (conj items value))
       :retract-fn (fn [items retracted] (drop-one-of items retracted))
       :combine-fn concat}))
  ([field]
     (accum
      {:initial-value []
       :reduce-fn (fn [items value] (conj items (field value)))
       :retract-fn (fn [items retracted] (drop-one-of items (field retracted)))
       :combine-fn concat})))
