(ns clara.rules.accumulators
  "A set of common accumulators usable in Clara rules."
  (:require [clara.rules.engine :as eng]
            [clojure.set :as set]
            [clara.rules :refer [accumulate]])
  (:refer-clojure :exclude [min max distinct count]))

(defn- drop-one-of
  "Removes one instance of the given value from the sequence."
  [items value]
  (let [pred #(not= value %)]
    (into (take-while pred items)
          (rest (drop-while pred items)))))

(defn- comparison-based
  "Creates a comparison-based result such as min or max"
  [field comparator returns-fact]
  (accumulate
   :reduce-fn (fn [values item]
                (conj values item))
   :combine-fn into
   :retract-fn (fn [values retracted] (drop-one-of values retracted))
   :convert-return-fn (fn [values]
                        (when-let [smallest (first (sort-by field comparator values))]
                          (if returns-fact
                            smallest
                            (field smallest))))))

(defn min
  "Returns an accumulator that returns the minimum value of a given field."
  [field & {:keys [returns-fact]}]
  (comparison-based field < returns-fact))

(defn max
  "Returns an accumulator that returns the maximum value of a given field."
  [field & {:keys [returns-fact]}]
  (comparison-based field > returns-fact))

(defn average
  "Returns an accumulator that returns the average value of a given field."
  [field]
  (accumulate
   :initial-value [0 0]
   :reduce-fn (fn [[value count] item]
                [(+ value (field item)) (inc count)])
   :retract-fn (fn [[value count] retracted]
                 [(- value (field retracted)) (dec count)])
   :combine-fn (fn [[value1 count1] [value2 count2]]
                 [(+ value1 value2) (+ count1 count2)])
   :convert-return-fn (fn [[value count]]
                        (if (= 0 count)
                          nil
                          (/ value count)))))

(defn sum
  "Returns an accumulator that returns the sum of values of a given field"
  [field]
  (accumulate
   :initial-value 0
   :reduce-fn (fn [total item]
                (+ total (field item)))
   :retract-fn (fn [total item]
                (- total (field item)))
   :combine-fn +))

(defn count
  "Returns an accumulator that simply counts the number of matching facts"
  []
  (accumulate
   :initial-value 0
   :reduce-fn (fn [count value] (inc count))
   :retract-fn (fn [count retracted] (dec count))
   :combine-fn +))

(defn distinct
  "Returns an accumulator producing a distinct set of facts.
   If given a field, returns a distinct set of values for that field."
  ([]
     (accumulate
      :initial-value #{}
      :reduce-fn (fn [items value] (conj items value))
      :retract-fn (fn [items retracted] (disj items retracted))
      :combine-fn set/union))
  ([field]
     (accumulate
      :initial-value #{}
      :reduce-fn (fn [items value] (conj items (field value)))
      :retract-fn (fn [items retracted] (disj items (field retracted)))
      :combine-fn set/union)))

(defn all
  "Returns an accumulator that preserves all accumulated items.
   If given a field, returns all values in that field."
  ([]
     (accumulate
      :initial-value []
      :reduce-fn (fn [items value] (conj items value))
      :retract-fn (fn [items retracted] (drop-one-of items retracted))
      :combine-fn concat))
  ([field]
     (accumulate
      :initial-value []
      :reduce-fn (fn [items value] (conj items (field value)))
      :retract-fn (fn [items retracted] (drop-one-of items (field retracted)))
      :combine-fn concat)))
