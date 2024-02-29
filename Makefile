.PHONY: repl test clean compile-main-java compile-test-java deploy install format-check format-fix

SHELL := /bin/bash

compile-main-java:
	clojure -T:build compile-main-java

compile-test-java: compile-main-java
	clojure -T:build compile-test-java

repl: compile-test-java
	clojure -M:dev:test:repl

test: compile-test-java
	clojure -M:dev:test:runner --focus :unit --reporter kaocha.report/documentation --no-capture-output

test-coverage: compile-test-java
	clojure -M:dev:test:runner --focus :coverage --reporter kaocha.report/documentation --no-capture-output
	jq '.coverage["clara/coverage_ruleset.clj"]|join("_")' target/coverage/codecov.json | grep "_1___1____1_0_1__1__" || echo "Unexpected coverage output for clara.coverage-ruleset." exit 1

test-generative: compile-test-java
	clojure -M:dev:test:runner --focus :generative --reporter kaocha.report/documentation --no-capture-output

test-config:
	clojure -M:dev:test:runner --print-config

clean:
	rm -rf target build

lint: compile-test-java
	clojure -M:dev:test:clj-kondo --copy-configs --dependencies --parallel --lint "$(shell clojure -A:dev:test -Spath)"
	clojure -M:dev:test:clj-kondo --lint "src/main:src/test" --fail-level "error"

build: compile-main-java
	clojure -X:jar :sync-pom true

deploy: clean build
	clojure -X:deploy-maven

install:
	clojure -X:install-maven

format-check:
	clojure -M:format-check

format-fix:
	clojure -M:format-fix
