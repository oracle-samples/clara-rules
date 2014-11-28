This is a history of changes to clara-rules.

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
