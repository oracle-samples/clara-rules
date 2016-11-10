(ns clara.sample-ruleset-seq
  "This namespace contains all productions in clara.sample-ruleset, but instead of containing them
  in individual rule and query structures they are contained in a seq that is marked as containing a sequence
  of productions.  This namespace exists so that tests can validate the functionality of scanning namespaces for
  rule sequences as discussed in https://github.com/cerner/clara-rules/issues/134"
  (:require [clara.sample-ruleset]))

(def ^:production-seq all-rules (->> (ns-interns 'clara.sample-ruleset)
                                     vals
                                     (filter #((some-fn :rule :query) (meta %)))
                                     (map deref)
                                     doall))
