# clara

Clara is a forward-chaining rules engine written in Clojure with Java interoperability. 

### The Need
The expression of arbitrary, frequently changing business logic is a major source of complexity. Clara aims to rein in this complexity by untangling business logic, expressing it as composable rules while leveraging the advantages of the Clojure and Java ecosystems. Clara should be usable as a Clojure library to simplify our logic or as an alternative to other Java-based rules engines like Drools.

Objectives include:

* Embrace immutability. The rule engine's working memory is a persistent Clojure data structure that can participate in transactions. All changes produce a new working memory that shares state with the previous.
* Rule constraints and actions are Clojure s-expressions.
* Working memory facts are typically Clojure records or Java objects following the Java Bean conventions. 
* Support the major advantages of existing rules systems, such as explainability of why a rule fired and automatic truth maintenance.
* Collections of facts can be reasoned with using accumulators similar to Jess or Drools. These accumulators leverage the reducers API and are transparently parallelized.
* The working memory is independent of the logic flow, and can be replaced with a distributed processing system. A [prototype that uses Storm](https://github.com/rbrush/clara-storm) to apply rules to a stream of incoming events already exists. Leveraging other processing infrastructures is possible.

### Logical Knots
Two things pop out as Clojure's strengths: the dramatic reduction of our systems' state space, and flexibility to model logic and program flow as simply as possible. Excellent Clojure libraries have emerged to make solving many classes of programs simpler, ranging from web development (ring, hiccup, compojure), to asynchronous program flow (core.async), to logic programming (core.logic), and many others.

This library is targeted at problems that lend themselves to forward-chaining rules. As we get better at managing state and program flow, our biggest source of complexity comes from what I call _logical knots_: arbitrary business logic with complex dependencies on other logic and data, with frequent changes that require deep dives into that logic. Even a functional approach to such problems can become unwieldy, with hierarchies of function calls too deep to easily reason about. Evolving business needs may require some function deep in the stack to suddenly require new input, forcing changes to multiple consumers. 

These are good cases for a rules approach. Instead of adding and updating functions for new business logic, we express logic independently of rules that run against a working memory of our data. We can reason about units of logic independently and apply updates without cascading changes.

### Example
Here we look at a simple example using Clara to simplify business logic. Readers familiar with existing rules engines like Jess or Drools may be interested in the more involved [sensors example](https://github.com/rbrush/clara-examples/blob/master/src/main/clojure/clara/examples/sensors.clj).

Imagine a retail setting where discounts and promotions come and go based on arbitrary criteria. We might have a special where anyone who buys a gizmo gets a free lunch, and express it simply:

```clj
(defrule free-lunch-with-gizmo
  "Anyone who purchases a gizmo gets a free lunch."
  [Purchase (= item :gizmo)]
  =>
  (insert! (->Promotion :free-lunch-with-gizmo :lunch)))
```

In rules engines terms, _Purchase_ is an example of a fact. Clara generally (but not exclusively) represents facts as simple Clojure records. We detect a gizmo purchase in our working memory, and insert the presence of a promotion for a free lunch. 

Now suppose our business declares August as "free widget month" for orders over $200. Rather than updating some "get-promotions" function to include this as well, we simply add another rule to our rulebase:

```clj
(defrule free-widget-month
  "All purchases over $200 in August get a free widget."
  [Order (= :august month)]
  [Total (> total 200)]
  =>
  (insert! (->Promotion :free-widget-month :widget)))
```

Note the constraints on facts are simply Clojure s-expressions that have access to the corresponding fact's fields. Furthermore, the _right-hand side_ (the part of the rule after the => symbol) is also simply an s-expression.

Users of the system can then query working memory for arbitrary facts. Here's a query that gets all of our promotions:

```clj
(defquery get-promotions
  "Query to find promotions for the purchase."
  []
  [?promotion <- Promotion])
```

More sophisticated rules and queries will use bound variables to join facts and select pertinent subsets. In this case we simply want the Promotion facts, so this query finds them and binds them to the _?promotion_ variable.

Consuming logic can then query the working memory for Promotion facts, returning a list of all promotions for display. Here's an example of how this could be invoked:

```clj
(-> (mk-session 'clara.examples.shopping) ; Load the rules.
    (insert (->Customer :vip)
            (->Order 2013 :march 20)
            (->Purchase 20 :gizmo)
            (->Purchase 120 :widget)) ; Insert some facts.
    (fire-rules)
    (query get-promotions {}))
```

This loads the set of rules and queries defined in the _clara.examples.shopping_ namespace, inserts some facts, fires all rules, and executes a query to get the promotions. The return value is a sequence containing items that matched the query.

Of course, this simple example doesn't demonstrate some of the more powerful aspects of a rules engine, including:

* Rule logic and constraints can be arbitrarily complicated, composed with boolean operators.
* Values in rules can be bound to variables, which the underlying engine will unify with other values.
* _Accumulators_ support reasoning over collections of facts, using Clojure reducers.

The [sensors example](https://github.com/rbrush/clara-examples/blob/master/src/main/clojure/clara/examples/sensors.clj) shows some of these features in action. 

There is also an [example using the Java API](https://github.com/rbrush/clara-examples/tree/master/src/main/java/clara/examples/java), which also demonstrates the use of Java Beans as facts.

## The Rules Engine
Clara implements a variation of the [Rete algorithm](http://en.wikipedia.org/wiki/Rete_algorithm), largely as described in the [Doorenbos paper](http://reports-archive.adm.cs.cmu.edu/anon/1995/CMU-CS-95-113.pdf). It does offer some enhancements and optimizations over classic Rete:

* The engine typically deals with collections of facts rather than individual items for efficiency.
* Accumulators similar to Jess and Drools allows users to write logic in terms of arbitrary collections of facts.
* All facts must be immutable. This simplifies the underlying system and makes it possible to distribute facts and processing over multiple machines and processes.

## Usage

Documentation to come. See the [clara-examples project](https://github.com/rbrush/clara-examples) for some examples.

## License

Copyright © 2013 Ryan Brush

Distributed under the Eclipse Public License, the same as Clojure.
