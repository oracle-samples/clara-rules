(defproject org.toomuchcode/clara-rules "0.11.1"
  :description "Clara Rules Engine"
  :url "https://github.com/rbrush/clara-rules"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/clojurescript "1.7.170" :scope "provided"]
                 [prismatic/schema "1.0.1"]]
  :plugins [[lein-codox "0.9.0" :exclusions [org.clojure/clojure]]
            [lein-javadoc "0.2.0" :exclusions [org.clojure/clojure]]
            [lein-cljsbuild "1.1.3" :exclusions [org.clojure/clojure]]
            [lein-figwheel "0.5.2" :exclusions [org.clojure/clojure]]]
  :codox {:namespaces [clara.rules clara.rules.dsl clara.rules.accumulators
                       clara.rules.listener clara.rules.durability
                       clara.tools.inspect clara.tools.tracing]
          :metadata {:doc/format :markdown}}
  :javadoc-opts {:package-names "clara.rules"}
  :source-paths ["src/main/clojure"]
  :resource-paths []
  :test-paths ["src/test/clojure" "src/test/common"]
  :java-source-paths ["src/main/java"]
  :javac-options ["-target" "1.6" "-source" "1.6"]
  :clean-targets ^{:protect false} ["resources/public/js" "target"]
  :hooks [leiningen.cljsbuild]
  :cljsbuild {:builds [;; Simple mode compilation for tests.
                       {:id "simple"
                        :source-paths ["src/test/clojurescript" "src/test/common"]
                        :figwheel true
                        :compiler {:main "clara.test"
                                   :output-to "resources/public/js/simple.js"
                                   :output-dir "resources/public/js/out"
                                   :asset-path "js/out"
                                   :optimizations :none}}

                       ;; Advanced mode compilation for tests.
                       {:id "advanced"
                        :source-paths ["src/test/clojurescript"]
                        :compiler {:output-to "target/js/advanced.js"
                                   :optimizations :advanced}}]

              :test-commands {"phantom-simple" ["phantomjs"
                                                "src/test/js/runner.js"
                                                "src/test/html/simple.html"]

                              "phantom-advanced" ["phantomjs"
                                                  "src/test/js/runner.js"
                                                  "src/test/html/advanced.html"]}}
  :scm {:name "git"
        :url "https://github.com/rbrush/clara-rules"}
  :pom-addition [:developers [:developer
                              [:id "rbrush"]
                              [:name "Ryan Brush"]
                              [:url "http://www.toomuchcode.org"]]]
  :deploy-repositories [["snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"
                                      :creds :gpg}]])
