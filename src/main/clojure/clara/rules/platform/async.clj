(ns clara.rules.platform.async
  (:require [clojure.core.async :refer [take!]])
  (:import [clojure.lang Var IDeref]
           [java.util.concurrent
            ExecutionException
            CompletableFuture
            ExecutorService
            ForkJoinPool
            Future]
           [clojure.core.async.impl.channels ManyToManyChannel]
           [java.util.function Function]))

(defprotocol CompletableFutureAdapter
  (as-completable-future [this]))

(defn completable-future-adapter?
  "is v a `CompletableFutureAdapter`?"
  [v]
  (satisfies? CompletableFutureAdapter v))

(def ^:private ^Function flatten-handler
  (reify Function
    (apply [_ v]
      (if-not (completable-future-adapter? v)
        (CompletableFuture/completedFuture v)
        (.thenCompose ^CompletableFuture (as-completable-future v) ^Function flatten-handler)))))

(defn flatten-completable-future
  "recursively takes from an async object until only one non-async value remains,
   returning it over a future"
  [^CompletableFuture f]
  (.thenCompose ^CompletableFuture f ^Function flatten-handler))

(defn- handle-async-channel-result
  "puts a result on an async channel if not nil, always closes the channel when done"
  [channel]
  (let [^CompletableFuture f (CompletableFuture.)]
    (take! channel
           (fn do-read
             [v]
             (cond
               (instance? ExecutionException v)
               (if-let [^Exception cause (ex-cause v)]
                 (.completeExceptionally f ^Exception cause)
                 (.completeExceptionally f ^Exception v))

               (instance? Exception v)
               (.completeExceptionally f ^Exception v)

               :else
               (.complete f v))))
    f))

(defn- handle-blocking-deref-result
  "puts a blocking ref result on an async channel if not nil, always closes the channel when done"
  [vref]
  (let [^CompletableFuture f (CompletableFuture.)]
    (try
      (.complete f @vref)
      (catch ExecutionException e
        (if-let [^Exception cause (ex-cause e)]
          (.completeExceptionally f ^Exception cause)
          (.completeExceptionally f ^Exception e)))
      (catch Exception e
        (.completeExceptionally f e)))
    f))

(extend-type CompletableFuture
  CompletableFutureAdapter
  (as-completable-future [f]
    f))

(extend-type ManyToManyChannel
  CompletableFutureAdapter
  (as-completable-future [c]
    (handle-async-channel-result c)))

(extend-type IDeref
  CompletableFutureAdapter
  (as-completable-future [r]
    (handle-blocking-deref-result r)))

(extend-type Future
  CompletableFutureAdapter
  (as-completable-future [f]
    (handle-blocking-deref-result f)))

(def ^:dynamic *thread-pool* (ForkJoinPool/commonPool))

(defmacro completable-future
  "Asynchronously invokes the body inside a completable future, preserves the current thread binding frame,
  using by default the `ForkJoinPool/commonPool`, the pool used can be specified via `*thread-pool*` binding."
  ^CompletableFuture [& body]
  `(let [binding-frame# (Var/cloneThreadBindingFrame) ;;; capture the thread local binding frame before start
         ^CompletableFuture res-fut# (CompletableFuture.) ;;; this is the CompletableFuture being returned
         ^ExecutorService pool# (or *thread-pool* (ForkJoinPool/commonPool))
         ^Runnable fbody# (fn do-complete#
                            []
                            (try
                              (Var/resetThreadBindingFrame binding-frame#) ;;; set the Clojure binding frame captured above
                              (.complete res-fut# (do ~@body)) ;;; send the result of evaluating the body to the CompletableFuture
                              (catch Exception ~'e
                                (.completeExceptionally res-fut# ~'e)))) ;;; if we catch an exception we send it to the CompletableFuture
         ^Future fut# (.submit pool# fbody#)
         ^Function cancel# (reify Function
                             (apply [~'_ ~'_]
                               (future-cancel fut#)))] ;;; submit the work to the pool and get the FutureTask doing the work
     ;;; if the CompletableFuture returns exceptionally
     ;;; then cancel the Future which is currently doing the work
     (.exceptionally res-fut# cancel#)
     res-fut#))
