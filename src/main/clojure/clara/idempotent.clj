(ns clara.idempotent
  "Implementation of insert-idempotent and defrecord-idempotent, such that duplicate identical facts will not be inserted."
  (:require [clara.rules :refer [fire-rules insert]]))

(defn dynamic-basis [record-class]
  (clojure.lang.Reflector/invokeStaticMethod (.getName record-class) "getBasis" (into-array [])))

(defprotocol ExactQueryable
  (present? [fact session]))

(defmacro prepare-idempotency [klass-sym]
  (let [klass (resolve klass-sym)
        fields (dynamic-basis klass)
        constraints (into [] (map #(-> `(~'= ~(symbol (str "?" %)) ~%)))     fields) 
        args        (into [] (comp (map #(str "?" (name %))) (map keyword))  fields)
        query-sym   (symbol (str "exact-match-"(.getSimpleName klass)))
        match-expr  (into [] cat (for [f fields] [(keyword (str "?" f)) (list (keyword f) 'fact)]))]
    `(do
       (defquery ~query-sym ~args [~'?f ~'<- ~klass-sym ~@constraints])
       (extend-type ~klass-sym ExactQueryable
                    (~'present? [~'fact ~'session]
                     (-> ~'session (query ~query-sym ~@match-expr)
                         first :?f))))))

(defn insert-idempotent [session fact]
  (if (present? fact session) session
      (-> session (insert fact) fire-rules)))

(defmacro defrecord-idempotent [& args]
  `(do
     (defrecord ~@args)
     (prepare-idempotency ~(first args))))
