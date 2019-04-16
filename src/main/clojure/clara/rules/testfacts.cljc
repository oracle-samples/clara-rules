(ns clara.rules.testfacts
  "This namespace exists primary for testing purposes, working around the fact that we cannot AOT compile test classes. This should be moved to the tests once a workaround for this is solved.")

;; Reflection against records requires them to be compiled AOT, so we temporarily
;; place them here as leiningen won't AOT compile test resources.
(defrecord Temperature [temperature location])
(defrecord WindSpeed [windspeed location])
(defrecord WindChill [temperature location wind-chill])
(defrecord Cold [temperature])
(defrecord Hot [temperature])
(defrecord ColdAndWindy [temperature windspeed])
(defrecord LousyWeather [])
(defrecord TemperatureHistory [temperatures])


;; Test facts for chained rules.
(defrecord First [])
(defrecord Second [])
(defrecord Third [])
(defrecord Fourth [])

;; Record utilizing clj flexible field names.
(defrecord FlexibleFields [it-works?
                           a->b
                           x+y
                           bang!])
