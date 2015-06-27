(defproject org.toomuchcode/clara-rules "0.9.0-SNAPSHOT"
  :description "Clara Rules Engine"
  :url "https://github.com/rbrush/clara-rules"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0-RC2"]
                 [org.clojure/tools.trace "0.7.8"]                  
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
                 [org.clojure/clojurescript "0.0-3308"]
                 [prismatic/schema "0.4.3"]
                 [dorothy "0.0.4"]
                 [hiccup "1.0.5"]]
  :plugins [[codox "0.8.10"]
            [lein-cljsbuild "1.0.6"]]
  :codox {:exclude [clara.other-ruleset clara.sample-ruleset clara.test-java
                    clara.test-rules clara.rules.memory clara.test-accumulators
                    clara.rules.testfacts clara.rules.java clara.rules.engine
                    clara.rules.compiler clara.rules.platform
                    clara.test-durability clara.tools.test-inspect clara.tools.test-tracing]}
  :source-paths ["src/main/clojure" "src/main/java"]
  :test-paths ["src/test/clojure"]
  :target-path "target"
  :java-source-paths ["src/main/java"]
  :hooks [leiningen.cljsbuild]
  :cljsbuild {:builds {:dev
                       {:source-paths ["src/main/clojurescript" "src/main/clojure"]
                        :jar true
                        :compiler {:pretty-print true
                                   :output-to "target/js/clara.js"
                                   :optimizations :whitespace}}}}
  :profiles {:dev {:dependencies [[cljsbuild "1.0.6"]]}
             :test {:cljsbuild {:builds 
                                {:test {:source-paths ["src/main/clojurescript" "src/test/clojurescript" "test"]
                                        :notify-command ["phantomjs" "phantom/unit-test.js" "phantom/unit-test.html"]
                                        :compiler {:output-to "target/cljs-test/testable.js"
                                                   :pretty-print true
                                                   :optimizations :whitespace}}}}}
             
             :aot {:aliases {"check" ["do" "clean," "compile"]}
                   :resource-paths ["/tmp/clara.rules/target/js/out"]
                   :target-path "/tmp/clara.rules/target/%s"
                   :compile-path "/tmp/clara.rules/target/classes"
                   :clean-targets ^{:protect false} ["/tmp/clara.rules/target"]
                   :aot :all
                   :cljsbuild {:builds 
                               {:dev {:compiler
                                      {:output-to "/tmp/clara.rules/target/js/out/clara.js"
                                       :output-dir "/tmp/clara.rules/target/js/out"
                                       :optimizations :whitespace}}}}}}
  
  :scm {:name "git"
        :url "https://github.com/rbrush/clara-rules.git"}
  :pom-addition [:developers [:developer
                              [:id "rbrush"]
                              [:name "Ryan Brush"]
                              [:url "http://www.toomuchcode.org"]]]
  :deploy-repositories [["snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"
                                      :creds :gpg}]])