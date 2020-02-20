(ns clara.rules.analysis-utils)

(defn is-variable?
  "Returns true if the given expression is a variable (a symbol prefixed by ?)"
  [expr]
  (and (symbol? expr)
       (.startsWith (name expr) "?")))
