(ns clara.rules.update-cache.core
  (:require [ham-fisted.api :as hf])
  (:import [ham_fisted MutList]))

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
(deftype OrderedUpdateCache [^MutList ^:unsynchronized-mutable updates]
  UpdateCache

  (add-insertions! [this facts]
    (.add updates (->PendingUpdate :insert facts)))

  (add-retractions! [this facts]
    (.add updates (->PendingUpdate :retract facts)))

  (get-updates-and-reset! [this]
    (let [current-updates (hf/persistent! updates)]
      (set! updates (hf/mut-list))
      (partition-by :type current-updates))))

(defn get-ordered-update-cache
  []
  (OrderedUpdateCache. (hf/mut-list)))
