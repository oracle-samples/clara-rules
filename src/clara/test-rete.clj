(ns clara.test-rete
  (use clojure.test
       clara.rete))

(defrecord Temperature [temperature])

(deftest test-create-cond
  (let [c (new-condition Temperature 
                         ['(< temperature 20)])]
    (is (= (->Condition Temperature 
                        ['(< temperature 20)] 
                        nil) 
           c))))

(deftest test-simple-parse-cond
  (let [c (parse-condition '(Temperature (< temperature 20)   ))]
    (is (= (->Condition Temperature 
                        ['(< temperature 20)] 
                        nil) 
           c))))

(deftest test-simple-production
  (let [p (new-production ['(Temperature (< temperature 20))] '(println "It's cold"))
        network (add-production (rete-network) p)]
    (println network)))


(deftest test-simple-alpha-compilation 
  (let [condition (parse-condition '(Temperature (< temperature 20)))
        test-fn (compile-alpha-test condition)
        compiled-fn (eval test-fn)]
    ;; Our test with a cold temperature should return bindings.
    (is (= {} (compiled-fn  (->Temperature 10))))
    ;; Our test against a hot temperature should return nil.
    (is (= nil (compiled-fn  (->Temperature 100))))))

(deftest test-simple-rete
  
  )


(run-tests)


