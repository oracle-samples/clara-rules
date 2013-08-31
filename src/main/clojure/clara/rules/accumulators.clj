(ns clara.rules.accumulators 
  "A set of common accumulators usable in Clara rules."
  (:require [clara.rules.engine :as eng] 
            [clojure.set :as set]
            [clara.rules :refer [accumulate]])
  (:refer-clojure :exclude [min max distinct count]))


(defn min
  "Returns an accumulator that returns the minimum value of a given field."
  [field & {:keys [returns-fact]}]
  (accumulate
   :reduce-fn (fn [value item]
                (if (or (= value nil)
                        (< (field item) (field value) ))
                  item
                  value))
   :convert-return-fn (if returns-fact
                        identity
                        #(field %))))

(defn max
  "Returns an accumulator that returns the maximum value of a given field."
  [field & {:keys [returns-fact]}]
  (accumulate
   :reduce-fn (fn [value item]
                (if (or (= value nil)
                        (> (field item) (field value) ))
                  item
                  value))
   :convert-return-fn (if returns-fact
                        identity
                        #(field %))))

(defn average
  "Returns an accumulator that returns the average value of a given field."
  [field]
  (accumulate 
   :initial-value [0 0]
   :reduce-fn (fn [[value count] item]
                [(+ value (field item)) (inc count)])
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
   :combine-fn +))

(defn count
  "Returns an accumulator that simply counts the number of matching facts"
  []
  (accumulate 
   :initial-value 0
   :reduce-fn (fn [count value] (inc count))
   :combine-fn +))

(defn distinct
  "Returns an accumulator producing a distinct set of facts.
   If given a field, returns a distinct set of values for that field."
  ([]
     (accumulate 
      :initial-value #{}
      :reduce-fn (fn [items value] (conj items value))
      :combine-fn (set/union)))
  ([field]
     (accumulate 
      :initial-value #{}
      :reduce-fn (fn [items value] (conj items (field value)))
      :combine-fn (set/union))))

