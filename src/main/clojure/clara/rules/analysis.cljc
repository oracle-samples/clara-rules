(ns clara.rules.analysis
  (:require [clara.rules.analysis-utils :as utils]))

(defn- warn-unused-binding
  [{:keys [lhs rhs] :as production}]
  (let [constraints-bindings (->> (filter utils/is-variable? (flatten (mapcat :constraints lhs))))
        fact-bindings (filter some? (flatten (map :fact-binding lhs)))
        rhs-bindings (filter utils/is-variable? (flatten rhs))])
  (println lhs)
  #_(let [re-binding #"\?[\w-]+"
          constraints-bindings (->> lhs
                                    (mapcat :constraints)
                                    (map (comp utils/is-variable? str))
                                    (filter some?))
          fact-bindings (->> lhs
                             (map :fact-binding)
                             (filter some?)
                             (map name))
          rhs-bindings (re-seq re-binding (str rhs))]
      (run! (fn [[?bind freq]]
              (when (= freq 1)
                (println (format "WARNING: binding %s defined at %s is not being used" ?bind (:name production)))))
            (->> constraints-bindings
                 (concat fact-bindings rhs-bindings)
                 (filter some?)
                 frequencies))))

(defn warn-unused-bindings!
  [productions]
  (run! warn-unused-binding productions))
