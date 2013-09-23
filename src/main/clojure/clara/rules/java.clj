(ns clara.rules.java
  "Java support. Users should use the Java API, or the clara.rules namespace from Clojure."
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

(defn- run-query [session name args]
  (let [query-var (or (resolve (symbol name))
                      (throw (IllegalArgumentException.
                              (str "Unable to resolve symbol to query: " name))))
        
        ;; Keywordize string keys from Java.
        keyword-args (into {}
                           (for [[k v] args]
                             [(keyword k) v]))
        results (eng/query session (deref query-var) keyword-args)]
    (map #(JavaQueryResult. %) results)))

(deftype JavaWorkingMemory [session]
  WorkingMemory

  (insert [this facts] 
    (JavaWorkingMemory. (apply clara/insert session facts)))

  (retract [this facts]
    (JavaWorkingMemory. (apply clara/insert session facts)))

  (fireRules [this]
    (JavaWorkingMemory. (clara/fire-rules session)))

  (query [this name args] 
   (run-query session name args))

  (query [this name]
     (run-query session name {})))

(defn mk-java-session [rulesets]
  (JavaWorkingMemory. 
   (apply clara/mk-session* (map symbol rulesets))))

