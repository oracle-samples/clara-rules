(ns clara.rules.platform
  "Code specific to the JavaScript platform.")

(defn throw-error
  "Throw an error with the given description string."
  [description]
  (throw (js/Error. description)))

;; The tuned group-by function is JVM-specific,
;; so just defer to provided group-by for ClojureScript.
(def tuned-group-by clojure.core/group-by)
