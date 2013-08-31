# clara

Clara is a forward-chaining rules engine written in Clojure with Java interoperability. 

The expression of arbitrary, frequently changing business logic is a major source of complexity. Clara aims to rein in this complexity by untangling business logic, expressing it as composable rules while leveraging the advantages of the Clojure and Java ecosystems. Clara should be usable as a Clojure library to simplify our logic or as an alternative to other Java-based rules engines like Drools.

Objectives include:

* Embrace immutability. The rule engine's working memory is a persistent Clojure data structure that can participate in transactions. All changes produce a new working memory that shares state with the previous.
* Rule constraints and actions are Clojure s-expressions.
* Working memory facts are typically Clojure records or Java objects following the Java Bean conventions. 
* Support the major advantages of existing rules systems, such as explainability of why a rule fired and automatic truth maintenance.
* Collections of facts can be reasoned with using accumulators similar to Jess or Drools. These accumulators leverage the reducers API and are transparently parallelized.
* The working memory is independent of the logic flow, and can be replaced with a distributed processing system. A [prototype that uses Storm](https://github.com/rbrush/clara-storm) to apply rules to a stream of incoming events already exists. Leveraging other processing infrastructures is possible.

## Resources

* The [introduction page](https://github.com/rbrush/clara-rules/wiki/Introduction) provides an overview of the project.
* The [architecture overview](https://github.com/rbrush/clara-rules/wiki/Architecture) goes into how Clara works.
* The [developer guide](https://github.com/rbrush/clara-rules/wiki/Guide) is in progress.
* See the [clara-examples project](https://github.com/rbrush/clara-examples) examples page for some examples.

## License

Copyright © 2013 Ryan Brush

Distributed under the Eclipse Public License, the same as Clojure.


