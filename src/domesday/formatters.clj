(ns domesday.formatters
  (:require [clojure-csv.core :refer [write-csv]]))


(defn- dump-csv [headers rows]
  (write-csv (vec (cons headers rows))))


(defn- format-course [row]
  (or (:course row) ""))

(defn- format-attempts [row]
  (str (:attempts row 0)))

(defn- format-completions [row]
  (str (:completions row 0)))

(defn- format-score [score]
  (if score
    (str (* 100.0 score) "%")
    ""))

(defn- activities-row [[id details] _]
  [(format-course details)
   (:name details)
   id
   (format-attempts details)
   (format-completions details)
   (format-score (:highest-score details))
   (format-score (:lowest-score details))])


(defn activity-summary
  [results agents]
  (dump-csv ["Course" "Activity Name" "Activity Id" "Attempts" "Completions" "Highest Score" "Lowest Score"]
    (loop [rows []
           [result & more] (seq results)]
      (if result
        (recur
          (conj rows
                (activities-row result agents))
          more)
        rows))))


(defn- format-actor-id [id]
  (cond
    (:mbox id) (subs (:mbox id) 7)
    (:mbox_sha1sum id) (:mbox_sha1sum id)
    (:account id) (str (-> id :account :name) " at " (-> id :account :homePage))
    (:openid id) (:openid id)
    :else (str id)))

(defn- format-actor-name [id agents]
  (or (:name (agents id)) (format-actor-id id)))

(defn- format-completed [row]
  (if (pos? (or (:completions row) 0))
    "Yes"
    "No"))

(defn- format-date-completed [row]
  (or (:completion-date row) ""))


(defn- actor-activities-rows
  [[actor-id activities] agents]
  (map (fn [[activity-id details]]
         [(format-actor-name actor-id agents)
          (format-actor-id actor-id)
          (format-course details)
          (:name details)
          activity-id
          (format-completed details)
          (format-date-completed details)
          (format-attempts details)
          (format-score (:highest-score details))])
       activities))


(defn activity-per-actor
  [results agents]
  (dump-csv ["Actor Name" "Actor Id" "Course" "Activity Name" "Activity Id" "Attempts" "Completed" "Date Completed" "Attempts" "Highest Score"]
    (loop [rows []
           [result & more] (seq results)]
      (if result
        (recur
          (into rows
                (actor-activities-rows result agents))
          more)
        rows))))
