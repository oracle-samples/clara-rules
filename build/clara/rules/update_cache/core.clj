(ns clara.rules.update-cache.core)

;; Record indicating pending insertion or removal of a sequence of facts.
(defrecord PendingUpdate [type facts])

;; This is expected to be used while activating rules in a given salience group
;; to store updates before propagating those updates to the alpha nodes as a group.
(defprotocol UpdateCache
  (add-insertions! [this facts])
  (add-retractions! [this facts])
  (get-updates-and-reset! [this]))

;; This cache replicates the behavior prior to https://github.com/cerner/clara-rules/issues/249,
;; just in a stateful object rather than a persistent data structure.
(deftype OrderedUpdateCache [updates]

  UpdateCache

  (add-insertions! [this facts]
    (swap! updates into [(->PendingUpdate :insert facts)]))

  (add-retractions! [this facts]
    (swap! updates into [(->PendingUpdate :retract facts)]))

  (get-updates-and-reset! [this]
    (let [current-updates @updates]
      (reset! updates [])
      (partition-by :type current-updates))))

(defn get-ordered-update-cache
  []
  (OrderedUpdateCache. (atom [])))
