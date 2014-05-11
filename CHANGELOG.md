This is a history of changes to clara-rules.

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
