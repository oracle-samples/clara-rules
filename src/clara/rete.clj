(ns clara.rete 
  (:require [clojure.reflect :as reflect]))

(defrecord Condition [type constraints bindings])

(defrecord Production [conditions rhs])

(defrecord Network [alpha-roots])

(defrecord AlphaNode [condition children activation])

(defrecord JoinNode [condition children])

(defrecord ProductionNode [action])

(defn get-fields 
  "Returns a list of fields in the given class."
  [cls]
  (map :name 
       (filter #(and (:type %) 
                     (not (#{'__extmap '__meta} (:name %)))   
                     (not (:static (:flags %)))) 
               (:members (reflect/type-reflect cls)))))

(deftype DummyRoot [])

(defn new-condition [type constraints]
  {:pre [(instance? java.lang.Class type)
         (vector? constraints)]}
  (->Condition type constraints nil))

(defn parse-condition 
  "Parse a condition, returning a Condition record."
  [condition]   
  {:pre [ (resolve (first condition))]}
  (new-condition (resolve (first condition)) (apply vector (rest condition))))

(defn new-production 
  [conditions rhs]
  (->Production (map parse-condition conditions) rhs ))

(defn compile-alpha-test 
  "Returns a function definition that can be used in alpha nodes to test the condition."
  [condition]
  ;; Todo: create a test that exercises all predicates and returns the bindings.
  ;; 
  (let [{:keys [type constraints]} condition
        fields (get-fields type)
        ;; Create an assignments vector for the let block.
        assignments (mapcat #(list 
                              % 
                              (list (symbol (str ".-" (name %))) 'this  )) 
                            fields)]
    `(fn [~'this] 
       (let [~@assignments
             bindings# {}] ; FIXME: bindings should be bindable!
         (if (and ~@constraints)
           bindings#
           nil)))))

(defn compile-unifications
  "Compiles a function that unifies a token and a new fact."
  [condition]
  nil)

(defn rete-network 
  "Creates an empty rete network."
  []
  (->Network #{}))

(defn- create-alpha-node [condition children]
  ;; TODO: Compile condition into alpha node.
  (->AlphaNode condition children (compile-alpha-test condition)))

(defn- create-join-node [condition children]
  ;; TODO: compile condition into beta node.
  (->JoinNode condition children))

(defn- add-production* 
  "Adds a new production, returning a tuple of a new beta root and a new set of alpha roots"
  [[condition & more] production alpha-roots]
  (if condition

    ;; Recursively create children, then create a new join and alpha node for the condition.
    (let [[child new-alphas] (add-production* more production alpha-roots)
          join-node (create-join-node condition [child])
          alpha-node (create-alpha-node condition [join-node])]
      
      [join-node (conj new-alphas alpha-node)])
    ;; No more conditions, so terminate the recursion by returning the production node.
    [production alpha-roots]))

(defn add-production 
  "Returns a new rete network identical to the given one, 
   but with the additional production."
  [network production]
  (let [[beta-root alpha-roots] (add-production* (:conditions production) 
                                                 production 
                                                 (:alpha-roots network)) ]
    (->Network alpha-roots)))





;; TODO: compile alpha and beta nodes with functions that accept a "working memory" object
;; that is used for all references. (Kind of cool that working memory is fully isolated from rete...)
