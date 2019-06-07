#?(:clj
   (ns clara.tools.testing-utils
     "Internal utilities for testing clara-rules and derivative projects.  These should be considered experimental
  right now from the perspective of consumers of clara-rules, although it is possible that this namespace 
  will be made part of the public API once its functionality has proven robust and reliable.  The focus, however,
  is functionality needed to test the rules engine itself."
     (:require [clara.rules.update-cache.core :as uc]
               [clara.rules.update-cache.cancelling :as ca]
               [clara.rules.compiler :as com]
               [clara.macros :as m]
               [clara.rules.dsl :as dsl]
               [clojure.test :refer [is]]))
   :cljs
   (ns clara.tools.testing-utils
     (:require [clara.rules.update-cache.core :as uc])
     (:require-macros [clara.tools.testing-utils]
       [cljs.test :refer [is]])))

#?(:clj
   (defmacro def-rules-test
     "This macro allows creation of rules, queries, and sessions from arbitrary combinations of rules
      and queries in a setup map without the necessity of creating a namespace or defining a session
      using defsession in both Clojure and ClojureScript.  The first argument is the name of the test, 
      and the second argument is a map with entries :rules, :queries, and :sessions.  For example usage see
      clara.test-testing-utils.  Note that sessions currently can only contain rules and queries defined
      in the setup map; supporting other rule sources such as namespaces and defrule/defquery may be possible
      in the future.

      Namespaces consuming this macro are expected to require clara.rules and either clojure.test or cljs.test.
      Unfortunately, at this time we can't add inline requires for these namespace with the macroexpanded code in
      ClojureScript; see https://anmonteiro.com/2016/10/clojurescript-require-outside-ns/ for some discussion on the 
      subject.  However, the test namespaces consuming this will in all likelihood have these dependencies anyway
      so this probably isn't a significant shortcoming of this macro."
     [name params & forms]
     (let [sym->rule (->> params
                          :rules
                          (partition 2)
                          (into {}
                                (map (fn [[rule-name [lhs rhs props]]]
                                       [rule-name (assoc (dsl/parse-rule* lhs rhs props {}) :name (str rule-name))]))))

           sym->query (->> params
                           :queries
                           (partition 2)
                           (into {}
                                 (map (fn [[query-name [params lhs]]]
                                        [query-name (assoc (dsl/parse-query* params lhs {}) :name (str query-name))]))))

           production-syms->productions (fn [p-syms]
                                          (map (fn [s]
                                                 (or (get sym->rule s)
                                                     (get sym->query s)))
                                               p-syms))

           session-syms->session-forms (->> params
                                            :sessions
                                            (partition 3)
                                            (into []
                                                  (comp (map (fn [[session-name production-syms session-opts]]
                                                               [session-name (production-syms->productions production-syms) session-opts]))
                                                        (map (fn [[session-name productions session-opts]]
                                                               [session-name (if (com/compiling-cljs?)
                                                                               (m/productions->session-assembly-form (map eval productions) session-opts)
                                                                               `(com/mk-session ~(into [(vec productions)]
                                                                                                       cat
                                                                                                       session-opts)))]))
                                                        cat)))

           test-form `(~(if (com/compiling-cljs?)
                          'cljs.test/deftest
                          'clojure.test/deftest)
                        ~name
                        (let [~@session-syms->session-forms
                              ~@(sequence cat sym->query)
                              ~@(sequence cat sym->rule)]
                          ~@forms))]
       test-form)))

#?(:clj
   (defn opts-fixture
     ;; For operations other than replace-facts uc/get-ordered-update-cache is currently
     ;; always used.  This fixture ensures that CancellingUpdateCache is tested for a wide
     ;; variety of different cases rather than a few cases cases specific to it.
     [f]
     (f)
     (with-redefs [uc/get-ordered-update-cache ca/get-cancelling-update-cache]
       (f))))

(defn join-filter-equals
  "Intended to be a test function that is the same as equals, but is not visible to Clara as such
  and thus forces usage of join filters instead of hash joins"
  [& args]
  (apply = args))

(def side-effect-holder (atom nil))

(defn side-effect-holder-fixture
  "Fixture to reset the side effect holder to nil both before and after tests.
   This should be used as a :each fixture."
  [t]
  (reset! side-effect-holder nil)
  (t)
  (reset! side-effect-holder nil))

#?(:clj
   (defn time-execution
     [func]
     (let [start (System/currentTimeMillis)
           _ (func)
           stop (System/currentTimeMillis)]
       (- stop start)))
   :cljs
   (defn time-execution
     [func]
     (let [start (.getTime (js/Date.))
           _ (func)
           stop (.getTime (js/Date.))]
       (- stop start))))

(defn execute-tests
  [func iterations]
  (let [execution-times (for [_ (range iterations)]
                          (time-execution func))
        sum #(reduce + %)
        mean (/ (sum execution-times) iterations)
        std (->
              (into []
                    (comp
                      (map #(- % mean))
                      (map #(Math/pow (double %) 2.0)))
                    execution-times)
              sum
              (/ iterations)
              Math/sqrt)]
    {:std (double std)
     :mean (double mean)}))

(defn run-performance-test
  "Created as a rudimentary alternative to criterium, due to assumptions made during benchmarking. Specifically, that
   criterium attempts to reach a steady state of compiled and loaded classes. This fundamentally doesn't work when the
   metrics needed rely on compilation or evaluation."
  [form]
  (let [{:keys [description func iterations mean-assertion verbose]} form
        {:keys [std mean]} (execute-tests func iterations)]
    (when verbose
      (println (str \newline "Running Performance tests for:"))
      (println description)
      (println "==========================================")
      (println (str "Mean: " mean "ms"))
      (println (str "Standard Deviation: " std "ms" \newline)))
    (is (mean-assertion mean)
        (str "Actual mean value: " mean))
    {:mean mean
     :std std}))

#?(:clj
   (defn ex-data-search [^Exception e edata]
     (loop [non-matches []
            e e]
       (cond
         ;; Found match.
         (= edata
            (select-keys (ex-data e)
                         (keys edata)))
         :success

         ;; Keep searching, record any non-matching ex-data.
         (.getCause e)
         (recur (if-let [ed (ex-data e)]
                  (conj non-matches ed)
                  non-matches)
                (.getCause e))

         ;; Can't find a match.
         :else
         non-matches))))

#?(:clj
   (defn get-all-ex-data
     "Walk a Throwable chain and return a sequence of all data maps
  from any ExceptionInfo instances in that chain."
     [e]
     (let [get-ex-chain (fn get-ex-chain [e]
                          (if-let [cause (.getCause e)]
                            (conj (get-ex-chain cause) e)
                            [e]))]

       (map ex-data
            (filter (partial instance? clojure.lang.IExceptionInfo)
                    (get-ex-chain e))))))

#?(:clj
   (defmacro assert-ex-data [expected-ex-data form]
     `(try
        ~form
        (is false
            (str "Exception expected to be thrown when evaluating: " \newline
                 '~form))
        (catch Exception e#
          (let [res# (ex-data-search e# ~expected-ex-data)]
            (is (= :success res#)
                (str "Exception msg found: " \newline
                     e# \newline
                     "Non matches found: " \newline
                     res#)))))))
