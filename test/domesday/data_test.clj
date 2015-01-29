(ns domesday.data-test
  (:require [clojure.test :refer :all]
            [clojure.test.check :as tc]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.clojure-test :refer [defspec]]
            [clojure.core.async :as async]
            [domesday.data :refer :all]
            [domesday.data.reports :refer [count-statements]]))


; TODO disable logging


(def compound (fn [inner-gen]
                  (gen/one-of [(gen/list inner-gen)
                               (gen/map gen/keyword inner-gen)])))
(def scalars (gen/one-of [gen/int gen/boolean]))
(def json-gen (gen/map gen/keyword (gen/recursive-gen compound scalars)))

(def async-timeout 1000)

(defspec tabulate-counts-statements
  1e1
  (prop/for-all [v (gen/not-empty (gen/vector json-gen))]
    (let [result-ch (-> v async/to-chan (tabulate {:all []}
                                                  {:count count-statements}))
          result (async/alts!! [result-ch (async/timeout async-timeout)])]
      (= (count v)
         (some-> result
                 first
                 :count
                 :all
                 (:count {})
                 (:any 0))))))