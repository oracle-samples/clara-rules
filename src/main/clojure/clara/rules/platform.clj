(ns clara.rules.platform
  "Code specific to the JVM platform.")

(defn throw-error 
  "Throw an error with the given description string."
  [description]
  (throw (IllegalArgumentException. description)))