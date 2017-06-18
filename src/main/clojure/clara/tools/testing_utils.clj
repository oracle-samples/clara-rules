(ns clara.tools.testing-utils
  "Internal utilities for testing clara-rules and derivative projects.  These should be considered experimental
  right now from the perspective of consumers of clara-rules, although it is possible that this namespace 
  will be made part of the public API once its functionality has proven robust and reliable.  The focus, however,
  is functionality needed to test the rules engine itself."
  (:require [clara.macros :as m]
            [clara.rules.dsl :as dsl]
            [clara.rules.compiler :as com]
            [clara.rules.update-cache.core :as uc]
            [clara.rules.update-cache.cancelling :as ca]))

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
                                    [rule-name (dsl/parse-rule* lhs rhs props {})]))))

        sym->query (->> params
                        :queries
                        (partition 2)
                        (into {}
                              (map (fn [[query-name [params lhs]]]
                                     [query-name (dsl/parse-query* params lhs {})]))))
        
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
                          ~@(sequence cat sym->query)]
                      ~@forms))]
    test-form))

(defn opts-fixture
  ;; For operations other than replace-facts uc/get-ordered-update-cache is currently
  ;; always used.  This fixture ensures that CancellingUpdateCache is tested for a wide
  ;; variety of different cases rather than a few cases cases specific to it.
  [f]
  (f)
  (with-redefs [uc/get-ordered-update-cache ca/get-cancelling-update-cache]
    (f)))
  
