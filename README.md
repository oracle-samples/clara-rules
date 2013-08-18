# clara

Clara is a forward-chaining rules engine in Clojure, with strong Java interoperability forthcoming. 

The expression of arbitrary, frequently changing business logic is a major source of complexity for many systems. Clara aims to reign in this complexity by untangling business logic into composable rules while leveraging the advantages of the Clojure and Java ecosystems.

Other objectives include:

* The rule engine's working memory is a persistent Clojure data structure that can participate in transactions.
* Working memory facts are typically Clojure records or (soon) Java beans. 
* Rule constraints and actions are Clojure s-expressions.
* Collections of facts can be reasoned with using accumulators similar to Jess or Drools. These accumulators leverage the reducers API and are transparently parallelized.
* The working memory is independent of the logic flow, and can be replaced with a distributed processing system. A [prototype that uses Storm](https://github.com/rbrush/clara-storm) to apply rules to a stream of incoming events already exists. Leveraging other processing infrastructures is possible.

Clara should be usable as a Clojure library to simplify our logic or as an alternative to other Java-based rules engines like Drools.

## Logical Knots
Two things pop out as Clojure's strengths: the dramatic reduction of our systems' state space, and the flexibility to represent logic and program flow as simply as possible. Excellent Clojure libraries have emerged to make solving many clases of programs simpler, ranging from web development (ring, hiccup, compojure), to asynchronous program flow (core.async), to logic programming (core.logic), and many others.

This library is targeted at problems that lend themselves to forward-chaining rules. As the state space of our systems shrink our biggest source of complexity comes from what I call logical knots: arbitrary business logic with complex dependencies on other logic and data, with frequent changes involving deep dives into that logic. A functional approach to these problems is helpful, but even that can get complicated. A hierarchy of function calls can get too deep to easily reason about. Evolving business needs may require some function deep in the stack to suddenly require new input, forcing changes to multiple consumers. 

These are good cases for a rules approach. Instead of adding and updating functions for new business logic, we express logic independently of rules that run against a working memory of our data. We can reason about units of logic independently and apply updates without cascading changes.

## Example
Here's a simple example. Imagine a retail setting where discounts and promotions come and go based on arbitrary criteria. We might have a special where anyone who buys a gizmo gets a free lunch, and express it simply:

```clj
(defrule free-lunch-with-gizmo
  "Anyone who purchases a gizmo gets a free lunch."
  [Purchase (= item :gizmo)]
  =>
  (insert! (->Promotion :free-lunch-with-gizmo :lunch)))
```

In rules engines terms, _Purchase_ is an example of a fact. Clara generally (but not exlusively) represents facts as simple Clojure records. We detect a gizmo purchase in our working memory, and insert the presence of a promotion for a free lunch. 

Now suppose our business declares August as free widget month with orders over $200. Rather than updating some "get-promotions" function to include this as well, we simply add another rule to our rulebase:

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
    (query get-best-discount {}))
```

This loads a set of rules and queries defined in the _clara.examples.shopping_ namespace, inserts some facts, fires all rules, and runs a query to get the best discount. The return value is a sequence containing items that matched the query.

Of course, this simple example doesn't demonstrate some of the more powerful aspects of a rules engine, including:

* Rule logic and constraints can be arbitrarily complicated, composed with boolean operators.
* Values in rules can be bound to variables, which the underlying engine will unify with other values.
* _Accumulators_ support reasoning over collections of facts, using Clojure reducers.

More examples of this can be found in the [clara-examples project](https://github.com/rbrush/clara-examples), with more sophisticated use cases to follow.

## Usage

Documentation to come. See the [clara-examples project](https://github.com/rbrush/clara-examples) for some examples.

## License

Copyright © 2013 Ryan Brush

Distributed under the Eclipse Public License, the same as Clojure.
