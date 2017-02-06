(ns clara.experimental
  "WARNING: Functionality in this namespace is subject to 
   breaking API changes or removal at any time.
   Users are encouraged to discuss their use-cases 
   on the mailing list, GitHub, etc. if
   production use of functionality in 
   this namespace is being considered."
  (:require [clara.rules.engine :as eng]))

(defn replace-facts
  "Given facts to insert and facts to retract from a session, perform both operations
   in ways intended to maximize the ability of these operations to cancel each other out.
   The semantics of this function are identical to retracting the retractions,
   inserting the insertions, and then firing the rules.  This function is expected to perform
   better than this sequence of operations though if the facts being retracted and inserted
   have similar impacts on the session.  As with all the top-level operations on sessions
   a new session with the modifications applied; the session passes to this function is 
   not mutated.
   
   See issue 249 for further discussion of this functionality."

  [session insertions retractions]

  (eng/replace-facts session insertions retractions))
