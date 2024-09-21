.PHONY: repl test clean compile-main-java compile-test-java deploy install format-check format-fix

SHELL := /bin/bash

env:
	env

compile-main-java:
	clojure -T:build compile-main-java

compile-test-java: compile-main-java
	clojure -T:build compile-test-java

repl: compile-test-java
	clojure -M:dev:test:app:repl

test: compile-test-java
	clojure -M:dev:test:app:runner --focus :unit --reporter kaocha.report/documentation --no-capture-output

test-coverage: compile-test-java
	clojure -M:dev:test:app:runner --focus :coverage --reporter kaocha.report/documentation --no-capture-output
	jq '.coverage["clara/coverage_ruleset.clj"]|join("_")' target/coverage/codecov.json | grep "_1___1____1_0_1__1__" || echo "Unexpected coverage output for clara.coverage-ruleset." exit 1

test-generative: compile-test-java
	clojure -M:dev:test:app:runner --focus :generative --reporter kaocha.report/documentation --no-capture-output

test-config:
	clojure -M:dev:test:app:runner --print-config

clean:
	rm -rf target build

lint: compile-test-java
	clojure -M:dev:test:app:clj-kondo --copy-configs --dependencies --parallel --lint "$(shell clojure -A:dev:test -Spath)"
	clojure -M:dev:test:app:clj-kondo --lint "src/main:src/test" --fail-level "error"

build: compile-main-java
	clojure -X:jar :sync-pom true :jar "build/clara-rules.jar"

build-native: build
	mkdir -p build/graalvm-config
	clojure -X:uberjar :sync-pom false :jar "build/clara-test-app.jar"
	java -agentlib:native-image-agent=config-output-dir=build/graalvm-config -jar "build/clara-test-app.jar" clara.main
	native-image -jar "build/clara-test-app.jar" \
		"-H:+ReportExceptionStackTraces" \
		"-H:+JNI" \
		"-H:EnableURLProtocols=http,https,jar" \
		"-J-Dclojure.spec.skip-macros=true" \
		"-J-Dclojure.compiler.direct-linking=true" \
		"--report-unsupported-elements-at-runtime" \
		"--verbose" \
		"--no-fallback" \
		"--no-server" \
		"--allow-incomplete-classpath" \
		"--trace-object-instantiation=java.lang.Thread" \
		"--features=clj_easy.graal_build_time.InitClojureClasses" \
		"-H:ConfigurationFileDirectories=build/graalvm-config" \
		"build/clara-test-app"

deploy: clean build
	clojure -X:deploy-maven

install:
	clojure -X:install-maven

format-check:
	clojure -M:format-check

format-fix:
	clojure -M:format-fix
