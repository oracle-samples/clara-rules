(ns build
  (:require [clojure.tools.build.api :as b]))

(def basis (b/create-basis {:project "deps.edn"}))

(defn- compile-java
  [sources classes]
  (b/javac {:src-dirs sources
            :class-dir classes
            :basis basis
            :javac-opts ["--release" "11"]}))

(defn compile-main-java
  [_]
  (compile-java ["src/main/java"] "target/main/classes"))

(defn compile-test-java
  [_]
  (compile-java ["src/test/java"] "target/test/classes"))
