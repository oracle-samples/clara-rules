(ns clara.main
  (:require
   [clara.rules :refer [defrule
                        insert!
                        insert
                        defquery
                        defhierarchy
                        defsession
                        query
                        fire-rules-async] :as r]
   [futurama.core :refer [!<! !<!! async]])
  (:gen-class)
  (:import
   [java.util.concurrent CompletableFuture]))

(defrule simple-transform-rule
  "simple rule that transforms an input into a result"
  [:my/input [{:keys [value]}]
   (= value ?value)]
  =>
  (insert! {:fact/type :my/result
            :result ?value}))

(defquery simple-output-query
  "simple query that returns an output fact"
  []
  [?output <- :my/output])

(defhierarchy simple-hierarchy
  "simple hierarchy of facts"
  :my/thing :my/input
  :my/result :my/output
  :my/input :my/fact
  :my/output :my/fact)

(defsession my-session
  'clara.main
  :fact-type-fn :fact/type)

(defn -main
  "this is a test to compile a bunch of async code into native code, pretty nasty"
  [& args]
  (println
   "async result:"
   (!<!!
    (async
     (let [session (!<! (-> my-session
                            (insert {:fact/type :my/thing
                                     :value args})
                            (fire-rules-async)))]
       (query session simple-output-query))))))
