(defproject org.toomuchcode/clara-rules "0.4.0"
  :description "Clara Rules Engine"
  :url "http://rbrush.github.io/clara-rules/"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.codehaus.jsr166-mirror/jsr166y "1.7.0"]
                 [org.clojure/clojurescript "0.0-2156"]
                 [prismatic/schema "0.1.9"]
                 [dorothy "0.0.4"]
                 [com.cemerick/piggieback "0.1.2"]
                 [hiccup "1.0.4"]]
  :plugins [[codox "0.6.4"]
            [lein-javadoc "0.1.1"]
            [lein-cljsbuild "1.0.0-alpha2"]
            [com.cemerick/clojurescript.test "0.2.1"]]  
  :codox {:exclude [clara.other-ruleset clara.sample-ruleset clara.test-java
                    clara.test-rules clara.rules.memory clara.test-accumulators
                    clara.rules.testfacts clara.rules.java clara.rules.engine
                    clara.rules.compiler clara.rules.platform]}
  :javadoc-opts {:package-names ["clara.rules"]}
  :source-paths ["src/main/clojure"]
  :test-paths ["src/test/clojure"]
  :java-source-paths ["src/main/java"]
  :hooks [leiningen.cljsbuild] 
  :cljsbuild {:builds [{:source-paths ["src/main/clojurescript"]
                        :jar true
                        :compiler {:pretty-print true
                                   :output-to "target/js/clara.js"
                                   :optimizations :advanced}}

                       ;; Build for unit tests.
                       {:source-paths ["src/main/clojurescript" "src/test/clojurescript"]
                        :compiler {:output-to "target/cljs/testable.js"
                                   :optimizations :advanced}}]
              :test-commands {"unit-tests" ["phantomjs" :runner
                                            "window.literal_js_was_evaluated=true"
                                            "target/cljs/testable.js"]}
              :crossovers [clara.rules.memory clara.rules.engine clara.rules.accumulators]
              :crossover-path "target/crossovers/clojurescript"
              :crossover-jar true}

  ;; Austin for the ClojureScript REPL.
  :profiles {:dev {:plugins [[com.cemerick/austin "0.1.3"]]}}

  ;; Include piggieback.
  :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}

  :scm {:name "git"
        :url "https://github.com/rbrush/clara-rules.git"}
  :pom-addition [:developers [:developer
                              [:id "rbrush"]
                              [:name "Ryan Brush"]
                              [:url "http://www.toomuchcode.org"]]]
  :deploy-repositories [["snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"
                                      :creds :gpg}]])
