(ns domesday.cli
  (:require [clojure.tools.cli :refer [parse-opts]]
            [cheshire.core :refer [parse-string]]
            [domesday.protocols :refer [->xAPIEndpoint]]
            [clojure.string :refer [join]]
            [clj-time.local :as l]
            [clojure.java.io :refer [as-url]]))


(defn is-url? [url]
  (try
    (as-url url)
    (catch Exception e
      false))
  true)


(def cli-options
  [["-e" "--endpoint URL" "xAPI statements endpoint"
    :validate [is-url? "Must be a URL"]]
   ["-u" "--user USER" "HTTP Basic user name"]
   ["-p" "--password PASSWORD" "HTTP Basic password"]
   ["-A" "--start START" "Start date"
    :parse-fn l/to-local-date-time]
   ["-Z" "--end END" "End date"
    :parse-fn l/to-local-date-time]
   ["-q" "--query QUERY" "Parameters query string"
    :default "&"]
   ["-h" "--help"]])


(defn- usage [options-summary]
  (str "Usage: domesday [options] groups_filenames

  Options:\n" options-summary))


(defn- exit 
  ([status-code text]
    (println text)
    (System/exit status-code))
  ([status-code]
   (exit status-code "")))

(defn- error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (join \newline errors)))


(defn- get-group [group-path]
  (with-open [reader (clojure.java.io/reader group-path)]
    (let [[name & agents] (line-seq reader)
          agents (doall (map #(parse-string % true) agents))]
      [name agents])))


(defn- get-groups [group-paths]
  (into {:all []}
        (map get-group group-paths)))


(defn get-opts [args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    ; TODO make error messages about these friendlier
    (assert (:endpoint options) "`--endpoint` must be specified") 
    (assert (:user options) "`--user` must be specified") 
    (assert (:password options) "`--password` must be specified") 
    (assert (:start options) "`--start` must be specified") 
    (assert (:end options) "`--end` must be specified") 

    ;; Handle help and error conditions
    (cond
      (:help options) (exit 0 (usage summary))
      errors (exit 1 (error-msg errors)))

    ; TODO make params parsing better
    {:options (assoc options :endpoint (->xAPIEndpoint
                                         (:endpoint options)
                                         (:user options)
                                         (:password options)
                                         (:query options)))
     :groups (get-groups arguments)}))
