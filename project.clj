(defproject org.toomuchcode/clara-rules "0.1.1"
  :description "Clara Rules Engine"
  :url "http://rbrush.github.io/clara-rules/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]]
  :plugins [[codox "0.6.4"]
            [lein-javadoc "0.1.1"]]  
  :codox {:exclude [clara.other-ruleset clara.sample-ruleset clara.test-java
                    clara.test-rules clara.rules.memory clara.test-accumulators
                    clara.rules.testfacts clara.rules.java clara.rules.engine]}
  :javadoc-opts {:package-names ["clara.rules"]}
  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure"]
  :java-source-paths ["src/main/java"])
