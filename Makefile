.PHONY: repl test clean compile-main-java compile-test-java deploy install format-check format-fix

SHELL := /bin/bash
VERSION := 0.9.0-SNAPSHOT

compile-main-java:
	clj -T:build compile-main-java

compile-test-java: compile-main-java
	clj -T:build compile-test-java

repl: compile-test-java
	clj -M:dev:test:repl

test: compile-test-java
	clj -M:dev:test:runner --focus :unit --reporter kaocha.report/tap

test-generative: compile-test-java
	clj -M:dev:test:runner --focus :generative --reporter kaocha.report/tap

test-config:
	clj -M:dev:test:runner --print-config

clean:
	rm -rf pom.xml target build

lint: compile-test-java
	clj -M:dev:test:clj-kondo --copy-configs --dependencies --parallel --lint "$(shell clj -A:dev:test -Spath)"
	clj -M:dev:test:clj-kondo --lint "src/main:src/test" --fail-level "error"

build: compile-main-java
	clj -Spom
	clj -X:jar \
		:sync-pom true \
		:group-id "k13labs" \
		:artifact-id "clara-rules" \
		:version '"$(VERSION)"'

deploy: clean build
	clj -X:deploy-maven

install:
	clj -X:install-maven

format-check:
	clj -M:format-check

format-fix:
	clj -M:format-fix
