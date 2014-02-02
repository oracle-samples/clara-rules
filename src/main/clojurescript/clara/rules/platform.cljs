(ns clara.rules.platform
  "Code specific to the JavaScript platform.")

(defn throw-error 
  "Throw an error with the given description string."
  [description]
  (throw (js/Error. description)))