(ns clara.rules.engine.transports
  (:require
    [clara.rules.engine.impl :as impl]
    [clara.rules.engine.wme :as wme] [clara.rules.platform :as platform]
    [clara.rules.memory :as mem] [clara.rules.listener :as l]))  

;; Simple, in-memory transport.
(deftype LocalTransport []
  impl/ITransport
  (send-elements [transport memory listener nodes elements]

    (doseq [node nodes
            :let [join-keys (impl/get-join-keys node)]]

      (if (> (count join-keys) 0)

        ;; Group by the join keys for the activation.
        (doseq [[join-bindings element-group] (platform/tuned-group-by #(select-keys (:bindings %) join-keys) elements)]
          (impl/right-activate node
                          join-bindings
                          element-group
                          memory
                          transport
                          listener))

        ;; The node has no join keys, so just send everything at once
        ;; (if there is something to send.)
        (when (seq elements)
          (impl/right-activate node
                          {}
                          elements
                          memory
                          transport
                          listener)))))

  (send-tokens [transport memory listener nodes tokens]

    (doseq [node nodes
            :let [join-keys (impl/get-join-keys node)]]

      (if (> (count join-keys) 0)
        (doseq [[join-bindings token-group] (platform/tuned-group-by #(select-keys (:bindings %) join-keys) tokens)]

          (impl/left-activate node
                         join-bindings
                         token-group
                         memory
                         transport
                         listener))

        ;; The node has no join keys, so just send everything at once.
        (when (seq tokens)
          (impl/left-activate node
                         {}
                         tokens
                         memory
                         transport
                         listener)))))

  (retract-elements [transport memory listener nodes elements]
    (doseq  [[bindings element-group] (group-by :bindings elements)
             node nodes]
      (impl/right-retract node
                     (select-keys bindings (impl/get-join-keys node))
                     element-group
                     memory
                     transport
                     listener)))

  (retract-tokens [transport memory listener nodes tokens]
    (doseq  [[bindings token-group] (group-by :bindings tokens)
             node nodes]
      (impl/left-retract  node
                     (select-keys bindings (impl/get-join-keys node))
                     token-group
                     memory
                     transport
                     listener))))