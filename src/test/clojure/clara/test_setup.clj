(ns clara.test-setup
  "Things we need to do in order to run tests with a smile"
  (:require [kaocha.hierarchy :as hierarchy]
            [pjstadig.humane-test-output :as hto]))

(hierarchy/derive! ::ignore :kaocha/known-key)

(hto/activate!)

(defn defuse-zero-assertions
  "Don't fail the test suite if we hide an `is` within a `doseq`.

  See also https://cljdoc.org/d/lambdaisland/kaocha/1.80.1274/doc/-clojure-test-assertion-extensions#detecting-missing-assertions"
  [event]
  (if (= (:type event) :kaocha.type.var/zero-assertions)
    (assoc event :type ::ignore)
    event))

