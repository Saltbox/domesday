(ns domesday.core
  (:require [taoensso.timbre :as timbre]
            [domesday.xapi :as xapi]
            [domesday.cli :refer [get-opts]]
            [domesday.data :as data :refer [tabulate]]
            [clojure.core.async :as async :refer [<!! chan]])
  (:gen-class))


(timbre/refer-timbre)


(def processors
  {"Statement Count" data/count-agents
   "Completed Activities" data/completed-activities})


(defn -main
  "I connect all the things and make them run."
  [& args]
  ;; work around dangerous default behaviour in Clojure
  (alter-var-root #'*read-eval* (constantly false))
  (debug "Starting")
  (let [{:keys [options groups]} (get-opts args)
        statements-ch (chan)
        result-ch (tabulate statements-ch groups processors)]

    (debug "Generated statements URL" (:endpoint options))
    (xapi/fetch-statements statements-ch (:endpoint options) [(:user options) (:password options)])
    ; TODO make this fancy
    (println "result" (<!! result-ch)))

  (System/exit 0))
