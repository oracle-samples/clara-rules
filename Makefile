.PHONY: repl test clean compile-main-java compile-test-java deploy install format-check format-fix

SHELL := /bin/bash
VERSION := 0.9.0-SNAPSHOT

compile-main-java:
	clojure -T:build compile-main-java

compile-test-java: compile-main-java
	clojure -T:build compile-test-java

repl: compile-test-java
	clojure -M:dev:test:repl

test: compile-test-java
	clojure -M:dev:test:runner --focus :unit --reporter kaocha.report/tap

test-generative: compile-test-java
	clojure -M:dev:test:runner --focus :generative --reporter kaocha.report/tap

test-config:
	clojure -M:dev:test:runner --print-config

clean:
	rm -rf pom.xml target build

lint: compile-test-java
	clojure -M:dev:test:clj-kondo --copy-configs --dependencies --parallel --lint "$(shell clojure -A:dev:test -Spath)"
	clojure -M:dev:test:clj-kondo --lint "src/main:src/test" --fail-level "error"

build: compile-main-java
	clojure -Spom
	clojure -X:jar \
		:sync-pom true \
		:group-id "com.github.k13labs" \
		:artifact-id "clara-rules" \
		:version '"$(VERSION)"'

deploy: clean build
	clojure -X:deploy-maven

install:
	clojure -X:install-maven

format-check:
	clojure -M:format-check

format-fix:
	clojure -M:format-fix
