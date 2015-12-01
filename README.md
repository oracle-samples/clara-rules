# Clara
[![Build Status](https://travis-ci.org/rbrush/clara-rules.svg?branch=master)](https://travis-ci.org/rbrush/clara-rules)

Clara is a forward-chaining rules engine written in Clojure with Java interoperability. It aims to simplify code with a developer-centric approach to expxert systems. More at [clara-rules.org](http://www.clara-rules.org).

## Example

Here's a simple example. The [clara-examples project](https://github.com/rbrush/clara-examples) shows more sophisticated rules and queries.

```clj
(ns clara.support-example
  (:require [clara.rules :refer :all]))

(defrecord SupportRequest [client level])

(defrecord ClientRepresentative [name client])

(defrule is-important
  "Find important support requests."
  [SupportRequest (= :high level)]
  =>
  (println "High support requested!"))

(defrule notify-client-rep
  "Find the client representative and send a notification of a support request."
  [SupportRequest (= ?client client)]
  [ClientRepresentative (= ?client client) (= ?name name)] ; Join via the ?client binding.
  =>
  (println "Notify" ?name "that"  ?client "has a new support request!"))

;; Run the rules! We can just use Clojure's threading macro to wire things up.
(-> (mk-session)
    (insert (->ClientRepresentative "Alice" "Acme")
            (->SupportRequest "Acme" :high))
    (fire-rules))

;;;; Prints this:

;; High support requested!
;; Notify Alice that Acme has a new support request!
```

## Releases
Clara releases are on [Clojars](https://clojars.org/). Simply add the following to your project:

[![Clojars Project](http://clojars.org/org.toomuchcode/clara-rules/latest-version.svg)](http://clojars.org/org.toomuchcode/clara-rules)

## Resources

* Documentation is at [clara-rules.org](http://www.clara-rules.org).
* Questions or suggestions for Clara can be posted on the [Clara Rules Google Group](https://groups.google.com/forum/?hl=en#!forum/clara-rules).

## License

Copyright © 2015 Ryan Brush

Distributed under the Eclipse Public License, the same as Clojure.
