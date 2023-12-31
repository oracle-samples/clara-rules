[![Build Status](https://github.com/k13labs/clara-rules/actions/workflows/clojure.yml/badge.svg)](https://github.com/cerner/clara-rules/actions/workflows/clojure.yml)

# _About_

Clara is a forward-chaining rules engine written in Clojure(Script) with Java interoperability. It aims to simplify code with a developer-centric approach to expert systems. See [clara-rules.org](http://www.clara-rules.org) for more.

NOTE: this fork only supports the JVM/Clojure, and not ClojureScript.

# _Usage_

Here's a simple example. Complete documentation is at [clara-rules.org](http://www.clara-rules.org/docs/firststeps/).

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

# _Building_

Clara is built, tested, and deployed using [Clojure Tools Deps](https://clojure.org/guides/deps_and_cli).

CMake is used to simplify invocation of some commands.

# _Availability_

Clara releases for this project are on [Clojars](https://clojars.org/). Simply add the following to your project:

[![Clojars Project](http://clojars.org/com.github.k13labs/clara-rules/latest-version.svg)](http://clojars.org/com.github.k13labs/clara-rules)

# _Communication_

- Questions about Clara rules can be posted to the [Clara Rules Google Group](https://groups.google.com/forum/?hl=en#!forum/clara-rules) or the [Slack channel](https://clojurians.slack.com/messages/clara/).
- For any other questions or issues about this Clara rules fork specifically feel free to browse or open a [Github Issue](https://github.com/k13labs/clara-rules/issues).

# Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md)

# LICENSE

Copyright 2023 Jose Gomez

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

&nbsp;&nbsp;&nbsp;&nbsp;http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
