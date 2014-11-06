(ns domesday.formatters
  (:require [clojure-csv.core :refer [write-csv]]))


(defn- dump-csv [headers rows]
  (write-csv (vec (cons headers rows))))


(defn- format-course [row]
  (str (:course row)))

(defn- format-counter [key row]
  (str (key row 0)))

(defn- format-score [score]
  (if score
    (str (* 100.0 score) "%")
    ""))

(defn- activities-row [[id details] _]
  [(str (:name details))
   (str id)
   (format-course details)
   (format-counter :attempts details)
   (format-counter :compeltions details)
   (format-counter :successes details)
   (format-counter :failures details)
   (format-score (:highest-score details))
   (format-score (:lowest-score details))])


(defn activity-summary
  [results agents]
  (dump-csv ["Activity Name" "Activity Id" "Course" "Attempts" "Completions" "Successes" "Failures" "Highest Score" "Lowest Score"]
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

(defn- format-first-date-completed [row]
  (or (:first-completion-date row) ""))

(defn- format-date-success [row]
  (or (:success-date row) ""))

(defn- format-first-date-success [row]
  (or (:first-success-date row) ""))


(defn- actor-activities-rows
  [[actor-id activities] agents]
  (map (fn [[activity-id details]]
         [(format-actor-name actor-id agents)
          (format-actor-id actor-id)
          (format-course details)
          (str (:name details))
          (str activity-id)
          (format-completed details)
          (format-first-date-completed details)
          (format-date-completed details)
          (format-counter :successes details)
          (format-counter :failures details)
          (format-first-date-success details)
          (format-date-success details)
          (format-counter :attempts details)
          (format-score (:highest-score details))])
       activities))


(defn activity-per-actor
  [results agents]
  (dump-csv ["Actor Name" "Actor Id" "Course" "Activity Name" "Activity Id" "Completed" "First Completed Date" "Latest Completed Date" "Successes" "Failures" "First Success Date" "Latest Success Date" "Attempts" "Highest Score"]
    (loop [rows []
           [result & more] (seq results)]
      (if result
        (recur
          (into rows
                (actor-activities-rows result agents))
          more)
        rows))))
