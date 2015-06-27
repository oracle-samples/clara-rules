(ns clara.tools.dev
  "Some support tools to debug stuff in ClojureScript space"
  (:require  #?(:clj [clojure.pprint :as pprint] :cljs [cljs.pprint :as pprint]))
  #?(:cljs (:import [goog.string StringBuffer])))

#?(:cljs
    (def print->console (.-log js/console)))

#?(:clj
    (defn println->stderr
      "Write values to stderr, useful when debugging macro expansion in CLJS space,
       the CLJS compiler does not carry out prints to *out* to the outside world"
      [& values]
      (binding [*out* *err*]
        (doseq [v values]
          (print v) (print " "))
        (println)))
    :cljs
    (defn println->stderr
      "Write values to console, useful from CLJS space."
      [& values]
      (print->console (apply pr-str values))))


#?(:clj
    (defn pprint->stderr
      "Pretty print values to stderr, useful when debugging macro expansiosn in CLJS space,
       the CLJS compiler does not carry out prints to *out* to the outside world"
      [& values]
      (binding [*out* *err*]
        (doseq [v values]
          (pprint/pprint v))))
    :cljs
    (defn pprint->stderr
      "Pretty print values to console, useful from CLJS space"
      [& values]
      (let [sb (StringBuffer.)
            w (StringBufferWriter. sb)]
        (doseq [v values]
          (pprint/pprint v w))
         (print->console (str sb)))))
      


