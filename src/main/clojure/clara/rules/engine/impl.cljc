(ns clara.rules.engine.impl)

;; Returns a new session with the additional facts inserted.
(defprotocol ISession
  (insert [session fact] "Inserts a fact.")
  (retract [session fact] "Retracts a fact.")
  (fire-rules [session] "Fires pending rules and returns a new session where they are in a fired state.")
  (query [session query params] "Runs a query agains the session.")
  (components [session] "Returns the components of a session as defined in the session-components-schema"))

;; Left activation protocol for various types of beta nodes.
(defprotocol ILeftActivate
  (left-activate [node join-bindings tokens memory transport listener])
  (left-retract [node join-bindings tokens memory transport listener])
  (description [node])
  (get-join-keys [node]))

;; Right activation protocol to insert new facts, connecting alpha nodes
;; and beta nodes.
(defprotocol IRightActivate
  (right-activate [node join-bindings elements memory transport listener])
  (right-retract [node join-bindings elements memory transport listener]))

;; Specialized right activation interface for accumulator nodes,
;; where the caller has the option of pre-reducing items
;; to reduce the data sent to the node. This would be useful
;; if the caller is not in the same memory space as the accumulator node itself.
(defprotocol IAccumRightActivate
  (pre-reduce [node elements] "Pre-reduces elements, returning a map of bindings to reduced elements.")
  (right-activate-reduced [node join-bindings reduced  memory transport listener] 
                          "Right-activate the node with items reduced in the above pre-reduce step."))

;; The transport protocol for sending and retracting items between nodes.
(defprotocol ITransport
  (send-elements [transport memory listener nodes elements])
  (send-tokens [transport memory listener nodes tokens])
  (retract-elements [transport memory listener nodes elements])
  (retract-tokens [transport memory listener nodes tokens]))

;; Protocol for activation of Rete alpha nodes.
(defprotocol IAlphaActivate
  (alpha-activate [node facts memory transport listener])
  (alpha-retract [node facts memory transport listener]))


