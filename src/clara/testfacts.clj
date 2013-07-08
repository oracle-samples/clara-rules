(ns clara.testfacts)

;; Reflection against records requires them to be compiled AOT, so we temporarily
;; place them here as leiningen won't AOT compile test resources.
(defrecord Temperature [temperature location])
(defrecord WindSpeed [windspeed location])
