(ns domesday.core
  (:require [taoensso.timbre :as timbre]
            [domesday.xapi :as xapi]
            [domesday.cli :refer [get-opts]]
            [domesday.formatters :as formatters]
            [domesday.data :as data]
            [clojure.core.async :as async :refer [<!! chan]])
  (:gen-class))


(timbre/refer-timbre)


(def processors
  {"Activities per Actor" data/by-activity-actor
   "Activity Summary" data/by-activity})

(def available-formatters
  {"Activity Summary" formatters/activity-summary
   "Activities per Actor" formatters/activity-per-actor
   :default str})


(defn gather-results [options groups]
  (let [source-statements-ch (chan)
        statements-ch (async/mult source-statements-ch)
        statement-tabulate-ch (chan)
        statement-agent-ch (chan)
        result-ch (data/tabulate statement-tabulate-ch groups processors)
        agents-ch (data/gather-agents statement-agent-ch)]

    (async/tap statements-ch statement-tabulate-ch)
    (async/tap statements-ch statement-agent-ch)
    (xapi/fetch-statements source-statements-ch (:endpoint options) [(:user options) (:password options)])
    [(<!! result-ch) (<!! agents-ch)]))

(defn -main
  "I connect all the things and make them run."
  [& args]
  ;; work around dangerous default behaviour in Clojure
  (alter-var-root #'*read-eval* (constantly false))
  (debug "Starting")
  (let [{:keys [options groups]} (get-opts args)]
    (debug "Generated statements URL" (:endpoint options))

    (let [[results agents] (profile :info :Domesday
                             (gather-results options groups))]
      (doseq [[formatter-name result] results]
        (let [formatter (get available-formatters formatter-name (:default available-formatters))]
          (doseq [[group-name group-result] result]
            (println "\n\n--------------------------------")
            (println formatter-name ":" group-name)
            (println "--------------------------------\n\n")
            (println (formatter group-result agents)))))))

  (System/exit 0))
