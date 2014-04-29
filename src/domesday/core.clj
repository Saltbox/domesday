(ns domesday.core
  (:require [clojure.tools.cli :refer [parse-opts]]
            [taoensso.timbre :as timbre]
            [clj-time.local :as l]
            [clojure.string :refer [split join]]
            [clojure.java.io :refer [as-file as-url]]
            [clojurewerkz.urly.core :refer [url-like]]
            [domesday.xapi :refer [fetch-statements]]
            [clojure.core.async :refer [<!! <! go]])
  (:gen-class))


(timbre/refer-timbre)


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


(defn usage [options-summary]
  (str "Usage: domesday [options] groups_filenames

  Options:\n" options-summary))


(defn exit 
  ([status-code text]
    (println text)
    (System/exit status-code))
  ([status-code]
   (exit status-code "")))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (join \newline errors)))


(defn -main
  "I connect all the things and make them run."
  [& args]
  ;; work around dangerous default behaviour in Clojure
  (alter-var-root #'*read-eval* (constantly false))
  (debug "Starting")
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (assert (:endpoint options) "`--endpoint` must be specified") 
    (assert (:user options) "`--user` must be specified") 
    (assert (:password options) "`--password` must be specified") 
    (assert (:start options) "`--start` must be specified") 
    (assert (:end options) "`--end` must be specified") 

    ;; Handle help and error conditions
    (cond
      (:help options) (exit 0 (usage summary))
      errors (exit 1 (error-msg errors)))

    (let [endpoint-url (-> options
                         :endpoint
                         url-like
                         (.mutateQuery
                           (join "&"
                                 (-> (split (:query options) #"&")
                                   (conj (str "since=" (:start options)))
                                   (conj (str "until=" (:end options)))))))]
      (debug "Generated statements URL" (str endpoint-url))

      (let [statement-channel (fetch-statements (str endpoint-url) [(:user options) (:password options)])]
        (<!! (go
               (loop [statement (<! statement-channel)]
                 (when statement
                   (println "statement" statement)
                   (recur (<! statement-channel))))

               (println "done")
               true)))))
  (exit 0))
