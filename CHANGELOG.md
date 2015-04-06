This is a history of changes to clara-rules.

### 0.8.7
* Properly qualify references to Java classes on the RHS of rules, supporting try/catch and static method calls. See [issue 104](https://github.com/rbrush/clara-rules/issues/104).
* Fix bug when retracting a subset of facts blocked by a negation rule. See [issue 105](https://github.com/rbrush/clara-rules/issues/105).

### 0.8.6
* Fix a collection of issues surrounding referencing bound variables in nested functions. See [issue 90](https://github.com/rbrush/clara-rules/issues/90) and items referenced from there.
* Fix a truth maintenance issue for accumulators that offer an initial value when there is nothing to accumulate over. See [issue 91](https://github.com/rbrush/clara-rules/issues/91).
* Fix bug that caused options to be dropped in cljs. See [issue 92](https://github.com/rbrush/clara-rules/pull/92).
* Allow explicitly specifying productions in CLJS. See [issue 94](https://github.com/rbrush/clara-rules/pull/94).
* Better handle macro-generated rules. See [issue 100](https://github.com/rbrush/clara-rules/pull/100).
* The :no-loop property now applies to facts retracted due to truth maintenance. See [issue 99](https://github.com/rbrush/clara-rules/issues/99).

### 0.8.5
* Fix specific filtered accumulator bug. See [issue 89](https://github.com/rbrush/clara-rules/pull/89).
* Allow binding variables in set and map literals. See [issue 88](https://github.com/rbrush/clara-rules/pull/88).
* Fix truth maintenance consistency when working with equal facts. See [issue 84](https://github.com/rbrush/clara-rules/issues/84).

### 0.8.4
* Ensure all truth maintenance updates are flushed. See [issue 83](https://github.com/rbrush/clara-rules/issues/83).

### 0.8.3
* Fix for truth maintenance when an accumulator produces a nil value. See [issue 79](https://github.com/rbrush/clara-rules/pull/79).
* Use bound facts in unification. See [issue 80](https://github.com/rbrush/clara-rules/issues/80).
* Improve inspection and explainability support in the clara.tools.inspect namespace.

### 0.8.2
* Batch up inserts done by rules and apply them as a group. See [issue 58](https://github.com/rbrush/clara-rules/issues/58).
* Optimize some internal functions based on real-world profiling.

### 0.8.1
* Fix stack overflow under workloads with many individually inserted facts. See [issue 76](https://github.com/rbrush/clara-rules/pull/76).

### 0.8.0
* Support for salience. See [issue 25](https://github.com/rbrush/clara-rules/issues/25).
* Rule compilation is significantly faster. See [issue 71](https://github.com/rbrush/clara-rules/issues/71).
* Handle use cases where there are a large number of retracted facts. See [issue 74](https://github.com/rbrush/clara-rules/pull/74).
* Add insert-all! and insert-all-unconditional!. See [issue 75](https://github.com/rbrush/clara-rules/issues/75).

### 0.7.0
* Allow bound variables to be used by arbitrary functions in subsequent conditions. See [issue 66](https://github.com/rbrush/clara-rules/issues/66)
* Add metadata to rule's right-hand side so we see line numbers in compilation errors and call stacks. See [issue 69](https://github.com/rbrush/clara-rules/issues/69)
* Improved memory consumption in cases where rules may be retracted and re-added frequently.

### 0.6.2
* Properly handle retractions in the presence of negation nodes; see [issue 67](https://github.com/rbrush/clara-rules/issues/67).
* Report error if the fact type in a rule appears to be malformed; see [issue 65](https://github.com/rbrush/clara-rules/issues/65).

### 0.6.1
* Reduce depth of nested function for [issue 64](https://github.com/rbrush/clara-rules/issues/64).
* Clean up reflection warnings.

### 0.6.0
* Several performance optimizations described in [issue 56](https://github.com/rbrush/clara-rules/issues/56) and [this blog post](http://www.toomuchcode.org/blog/2014/06/16/micro-bench-macro-optimize/).
* Session durability as an experimental feature. See [issue 16](https://github.com/rbrush/clara-rules/issues/16) and [the wiki](https://github.com/rbrush/clara-rules/wiki/Durability).
* Improved ability to inspect session state and explain why rules and queries were activated. See the [inspection page on the wiki](https://github.com/rbrush/clara-rules/wiki/Inspection) for details.
* A list of smaller changes can be seen via the [milestone summary](https://github.com/rbrush/clara-rules/issues?milestone=8&page=1&state=closed)

### 0.5.0
Contains several bug fixes and some usage enhancements, including:

* The clara.tools.inspect package allows for inspection and explanation of why rules fired. Also see [issue 48](https://github.com/rbrush/clara-rules/issues/48).
* [File and line information is preserved when compiling.](https://github.com/rbrush/clara-rules/pull/51)
* A list of several other fixes can be seen via the [milestone summary](https://github.com/rbrush/clara-rules/issues?milestone=7&page=1&state=closed)

### 0.4.0
This is a major refactoring of the Clara engine, turning all rule and Rete network representations into well-defined data structures. Details are at these links:

* [Rules as Data Support](http://www.toomuchcode.org/blog/2014/01/19/rules-as-data/)
* [ClojureScript Rete network on the server side](https://github.com/rbrush/clara-rules/issues/34)

### 0.3.0
* [ClojureScript support](https://github.com/rbrush/clara-rules/issues/4)
* [No-loop option for rules](https://github.com/rbrush/clara-rules/issues/23)
* [Improved variable binding](https://github.com/rbrush/clara-rules/pull/26)

### 0.2.2
* [Accumulators should always fire if all variables can be bound](https://github.com/rbrush/clara-rules/issues/22)

### 0.2.1
A fix release with some internal optimizations, including the following:

* [Use activation list for rules](https://github.com/rbrush/clara-rules/issues/19)
* [Support symbols with "-" in them](https://github.com/rbrush/clara-rules/issues/20)

### 0.2.0
* [Remove need for == macro](https://github.com/rbrush/clara-rules/pull/18)
* [Support for arbitrary Clojure maps](https://github.com/rbrush/clara-rules/issues/6)

### 0.1.1
A number of bug fixes, see the [milestone summary](https://github.com/rbrush/clara-rules/issues?milestone=1&page=1&state=closed)

### 0.1.0
The initial release.
