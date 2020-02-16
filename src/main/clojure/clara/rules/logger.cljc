(ns clara.rules.logger)

(defn- warn-unused-binding
  [{:keys [lhs rhs] :as production}]
  (let [re-binding #"\?[\w-]+"
        constraints-bindings (->> lhs
                                  (mapcat :constraints)
                                  (map (comp (partial re-find re-binding) str))
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
