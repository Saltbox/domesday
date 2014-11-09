(ns domesday.core
  (:require [taoensso.timbre :as timbre]
            [domesday.xapi :as xapi]
            [domesday.cli :refer [get-opts]]
            [domesday.formatters :as formatters]
            [domesday.data :as data]
            [domesday.data.reports :as r]
            [domesday.protocols :refer [fetch]]
            [clojure.core.async :as async :refer [<!! chan]])
  (:gen-class))

(timbre/refer-timbre)


(def base-reports
  {"Activities per Actor" r/by-activity-actor
   "Activity Summary" r/by-activity})


(def available-formatters
  {"Activity Summary" formatters/activity-summary
   "Activities per Actor" formatters/activity-per-actor
   :default str})


(defn gather-results [endpoint start end groups reports]
  (let [source-statements-ch (chan)
        statements-ch (async/mult source-statements-ch)
        statement-tabulate-ch (chan)
        statement-agent-ch (chan)
        result-ch (data/tabulate statement-tabulate-ch groups reports)
        agents-ch (data/gather-agents statement-agent-ch)]

    (async/tap statements-ch statement-tabulate-ch)
    (async/tap statements-ch statement-agent-ch)
    (fetch endpoint source-statements-ch start end)
    [(<!! result-ch) (<!! agents-ch)]))


(defn domesday
  ([options groups] (domesday options groups base-reports))
  ([{:keys [endpoint start end]} groups reports]
   (let [[results agents]
         (gather-results endpoint start end groups reports)]
     (into {}
           (map (fn [[formatter-name result]]
                  [formatter-name
                   (let [formatter (get available-formatters
                                        formatter-name
                                        (:default available-formatters))]
                     (into {} (map (fn [[group-name group-result]]
                                     [group-name
                                      (formatter group-result agents)]) result)))])
                results)))))


(defn -main
  "I connect all the things and make them run."
  [& args]
  ;; work around dangerous default behaviour in Clojure
  (alter-var-root #'*read-eval* (constantly false))
  (debug "Starting")
  (let [{:keys [options groups]} (get-opts args)]
    (debug "Generated statements URL" (:endpoint options))

    (doseq [[formatter-name result] (domesday options groups)]
      (doseq [[group-name group-result] result]
        (println "\n\n--------------------------------")
        (println formatter-name ":" group-name)
        (println "--------------------------------\n\n")
        (println group-result))))

  (System/exit 0))
