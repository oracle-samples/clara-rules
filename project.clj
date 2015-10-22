(defproject org.toomuchcode/clara-rules "0.9.0-SNAPSHOT"
  :description "Clara Rules Engine"
  :url "https://github.com/rbrush/clara-rules"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/clojurescript "1.7.145" :scope "provided"]
                 [prismatic/schema "1.0.1"]
                 [dorothy "0.0.6"]
                 [hiccup "1.0.5"]]
  :plugins [[codox "0.8.10"]
            [lein-javadoc "0.2.0"]
            [lein-cljsbuild "1.1.0"]]
  :codox {:exclude [clara.other-ruleset clara.sample-ruleset clara.test-java
                    clara.test-rules clara.rules.memory clara.test-accumulators
                    clara.rules.testfacts clara.rules.java clara.rules.engine
                    clara.rules.compiler clara.rules.platform
                    clara.test-durability clara.tools.test-inspect clara.tools.test-tracing]}
  :javadoc-opts {:package-names "clara.rules"}

  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure" "src/test/common"]
  :java-source-paths ["src/main/java"]
  :hooks [leiningen.cljsbuild]
  :cljsbuild {:builds [;; Simple mode compilation for tests.
                       {:source-paths ["src/test/clojurescript" "src/test/common"]
                        :compiler {:output-to "target/js/simple.js"
                                   :optimizations :whitespace}}

                       ;; Advanced mode compilation for tests.
                       {:source-paths ["src/test/clojurescript"]
                        :compiler {:output-to "target/js/advanced.js"
                                   :optimizations :advanced}}]

              :test-commands {"phantom-simple" ["phantomjs"
                                                "src/test/js/runner.js"
                                                "src/test/html/simple.html"]

                              "phantom-advanced" ["phantomjs"
                                                  "src/test/js/runner.js"
                                                  "src/test/html/advanced.html"]}}

  :scm {:name "git"
        :url "https://github.com/rbrush/clara-rules.git"}
  :pom-addition [:developers [:developer
                              [:id "rbrush"]
                              [:name "Ryan Brush"]
                              [:url "http://www.toomuchcode.org"]]]
  :deploy-repositories [["snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"
                                      :creds :gpg}]])
