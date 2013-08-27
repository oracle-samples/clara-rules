(ns clara.rules.java
  (:require [clara.rules :as clara]
            [clara.rules.engine :as eng]
            [clara.rules.memory :as mem])
  (:refer-clojure :exclude [==])
  (import [clara.rules.engine LocalTransport]
          [clara.rules WorkingMemory QueryResult]))

(deftype JavaQueryResult [result]
  QueryResult
  (getResult [_ fieldName] 
    (get result (keyword fieldName)))
  Object
  (toString [_]
    (.toString result)))

(deftype JavaWorkingMemory [session]
  WorkingMemory

  (insert [this facts] 
    (JavaWorkingMemory. (apply clara/insert session facts)))

  (retract [this facts]
    (JavaWorkingMemory. (apply clara/insert session facts)))

  (fireRules [this]
    (JavaWorkingMemory. (clara/fire-rules session)))

  (query [this name args] 
    (let [query-var (or (resolve (symbol name))
                        (throw (IllegalArgumentException. 
                                 (str "Unable to resolve symbol to query: " name))))

          ;; Keywordize string keys from Java.
          keyword-args (into {} 
                             (for [[k v] args]
                               [(keyword k) v]))
          results (clara/query session (deref query-var) keyword-args)]
      (map #(JavaQueryResult. %) results))))

(defn mk-java-session [rulesets]
  (JavaWorkingMemory. 
   (apply clara/mk-session (map symbol rulesets))))

