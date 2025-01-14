[![Build Status](https://github.com/cerner/clara-rules/actions/workflows/clojure.yml/badge.svg)](https://github.com/cerner/clara-rules/actions/workflows/clojure.yml)

# Project name


## _About_

Clara is a forward-chaining rules engine written in Clojure(Script) with Java interoperability. It aims to simplify code with a developer-centric approach to expert systems. See [clara-rules.org](http://www.clara-rules.org) for more.

## _Usage_

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

## _Building_

Clara is built, tested, and deployed using [Leiningen](http://leiningen.org). 
ClojureScript tests are executed via [puppeteer](https://pptr.dev/). 
``` 
npm install -g puppeteer
npx puppeteer browsers install chrome
```

## _Availability_

Clara releases are on [Clojars](https://clojars.org/). Simply add the following to your project:

[![Clojars Project](http://clojars.org/com.cerner/clara-rules/latest-version.svg)](http://clojars.org/com.cerner/clara-rules)

## _Communication_

Questions can be posted to the [Clara Rules Google Group](https://groups.google.com/forum/?hl=en#!forum/clara-rules) or the [Slack channel](https://clojurians.slack.com/messages/clara/).  

## Contributing

This project welcomes contributions from the community. Before submitting a pull request, please [review our contribution guide](./CONTRIBUTING.md)

## Security

Please consult the [security guide](./SECURITY.md) for our responsible security vulnerability disclosure process

## License

Copyright (c) 2018, 2025 Oracle and/or its affiliates.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at

&nbsp;&nbsp;&nbsp;&nbsp;http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.


