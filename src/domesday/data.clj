(ns domesday.data
  (:require [taoensso.timbre :as timbre]
            [clojure.core.async :as async :refer [<! >! go chan sub pub go-loop]]
            [domesday.xapi :as xapi])
  (:import [org.joda.time Period]))

(timbre/refer-timbre)


(defn- process-statements [statements-channel processor-fn]
  "Process all the statements from the async channel using the
   processor function, yielding the result in the returned channel."
  ; A processor function should take either two arguments or zero.
  ;  If two arguments, they are the accumulated data and the next
  ;   Experience API statement.
  ;  If zero arguments, the processor function should return an initial
  ;   data structor that it will accept as its first argument.
  (debug "in process-statements for" processor-fn)
  (async/reduce processor-fn (processor-fn) statements-channel))


(defn- by-group-name [groups statement]
  (let [actor (:actor statement)
        group-names (map first (filter (fn [[group-name agents]]
	                                 (some (partial xapi/same-agent? actor) agents))
                                       groups))]
    (if (empty? group-names)
      [:catch-all :all]
      (conj group-names :all))))


(defn gather-agents [statement-ch]
  "Merge all statement actors into a single sequence of agents, yielding
   the sequence in the returned channel."
  (go-loop [agents {}]
    (if-let [statement (<! statement-ch)]
      (recur (update-in agents [(xapi/actor statement)]
                        (fn [agent]
                          (if (and (not (:name agent)) (:name (:actor statement)))
                            (:actor statement)
                            agent))))
      agents)))


(defn dispatch-group-channels
  "Create a new channel for each group, returning a map from group name to the
  corresponding group channel. This function dispatches statements to all
  matching group channels."
  [statement-ch groups]
  ; map group names to new channels
  (let [group-chs (into {} (map #(vec [% (chan)]) (keys groups)))
        group-mult-chs (into {} (map #(vec [% (async/mult (group-chs %))]) (keys groups)))]
    (debug "Constructed" (count group-chs) "channels for groups")
    ; Dispatch statements to all appropriate group channels while
    ; statement channel is open.
    (go-loop []
      (if-let [statement (<! statement-ch)]
        (do
          ; call (by-group-name groups statement) which returns a sequence of
          ; group names
          ; doseq over return, >! return-val statement
          (doseq [group-name (by-group-name groups statement)]
            (when (group-chs group-name)
              (>! (group-chs group-name) statement)))
          (recur))

        ; No futher statements, close all the channels
        (doseq [ch (vals group-chs)]
          (async/close! ch))))
    group-mult-chs))


(defn- process-group [processor-fn [group-name ch]]
  (let [per-process-group-ch (chan)]
    (async/tap ch per-process-group-ch)
    (go
      [group-name (<! (process-statements per-process-group-ch processor-fn))])))


(defn- async-map-map [f coll]
  (async/into {} (async/merge (map f coll))))


(defn tabulate [statement-ch groups processors]
  ; groups is a map from group-name to a list of agents
  ; processors is a map from processor-name to processor function
  ; do things
  ; yield per-group output of processors in the returned channel
  (debug "Entering tabulate")
  (let [group-chs (dispatch-group-channels statement-ch groups)
        groups (or (keys groups) [:all])]
    (debug "Constructed group channels" group-chs)
    (async-map-map
      (fn [[processor-name processor-fn]]
        (debug "Processing for" processor-name)
        (go
          [processor-name
           (<! (async-map-map (partial process-group processor-fn) group-chs))]))
      processors)))


; Here's a really simple processor, which counts the number of statements in
; the group it processes.
(defn count-agents
  ([]
     0)
  ([acc statement]
     (inc acc)))


;(defn- update-completions [activities statement]
;   (if (xapi/completed-activity? statement)
;     (update-in activities [(-> statement :object :id) (xapi/actor statement) :completions] #(if %1 (inc %1) 1))
;     activities))
;
;(defn- update-successes [activities statement]
;   (if (xapi/successful-activity? statement)
;     (update-in activities [(-> statement :object :id) (xapi/actor statement) :successes] #(if %1 (inc %1) 1))
;     activities))
;
;
;(defn- add-duration
;  ([]
;   0)
;  ([milliseconds iso8601-duration]
;   (+ milliseconds (.getMillis (.toStandardDuration (Period/parse iso8601-duration))))))
;
;
;(defn- update-durations [activities statement]
;  (if-let [duration (-> statement :result :duration)]
;    (update-in activities [(-> statement :object :id) (xapi/actor statement) :total-duration] #(if-not (nil? %1) (add-duration %1 (-> statement :result :duration)) (add-duration)))
;    activities))
;
;
;(defn- save-score [activities statement]
;  (if-let [score (-> statement :result :score)]
;    (let [timestamp (:timestamp statement)]
;      (update-in activities [(-> statement :object :id) (xapi/actor statement) :scores] #(conj (or % []) [score timestamp])))
;    activities))
;
;
;(defn completed-activities
;  ([]
;   {})
;  ([activities statement]
;   (-> activities
;     (update-completions statement)
;     (update-successes statement)
;     (save-score statement)
;     (update-durations statement))))


(defn- update-description [activities path statement]
  (update-in activities (conj path :name)
             (fn [existing-name]
               (or existing-name
                   (-> statement :object :definition :name xapi/get-lang)
                   (-> statement :object :id)))))

(defn- update-course [activities path statement]
  (update-in activities (conj path :course)
             (fn [course-name]
               (or course-name
                   (some-> statement :context :contextActivities :parent (get 0) :definition :name xapi/get-lang)))))

(defn- update-attempts [activities path statement]
  (update-in activities (conj path :attempts)
             (fn [attempts]
               (if attempts
                 (inc attempts)
                 1))))

(defn- update-completions [activities path statement]
  (update-in activities (conj path :completions)
             (fn [completions]
               (if (xapi/completed-activity? statement)
                 (if completions
                   (inc completions)
                   1)
                 (or completions 0)))))

(defn- update-successes [activities path statement]
  (update-in activities (conj path :successes)
             (fn [successes]
               (if (xapi/successful-activity? statement)
                 (if successes
                   (inc successes)
                   1)
                 (or successes 0)))))

(defn- update-completion-date [activities path statement]
  (update-in activities (conj path :completion-date)
             (fn [completion-date]
               (if (xapi/completed-activity? statement)
                 (let [d1 (or completion-date (:timestamp statement))
                       d2 (or (:timestamp statement) completion-date)]
                   ; if d1 is more recent than d2
                   ; dates are both ISO8601 format
                   (if (> (compare d1 d2) 0)
                     d1
                     d2))
                 completion-date))))

(defn- update-first-completion-date [activities path statement]
  (update-in activities (conj path :first-completion-date)
             (fn [completion-date]
               (if (xapi/completed-activity? statement)
                 (let [d1 (or completion-date (:timestamp statement))
                       d2 (or (:timestamp statement) completion-date)]
                   ; if d2 is more recent than d1
                   ; dates are both ISO8601 format
                   (if (< (compare d1 d2) 0)
                     d1
                     d2))
                 completion-date))))

(defn- update-success-date [activities path statement]
  (update-in activities (conj path :success-date)
             (fn [success-date]
               (if (xapi/successful-activity? statement)
                 (let [d1 (or success-date (:timestamp statement))
                       d2 (or (:timestamp statement) success-date)]
                   ; if d1 is more recent than d2
                   ; dates are both ISO8601 format
                   (if (> (compare d1 d2) 0)
                     d1
                     d2))
                 success-date))))

(defn- update-first-success-date [activities path statement]
  (update-in activities (conj path :first-success-date)
             (fn [success-date]
               (if (xapi/successful-activity? statement)
                 (let [d1 (or success-date (:timestamp statement))
                       d2 (or (:timestamp statement) success-date)]
                   ; if d2 is more recent than d1
                   ; dates are both ISO8601 format
                   (if (< (compare d1 d2) 0)
                     d1
                     d2))
                 success-date))))

(defn- update-highest-score [activities path statement]
  (update-in activities (conj path :highest-score)
             (fn [highest-score]
               (let [score (xapi/get-score statement)]
                 (if (nil? score)
                   highest-score
                   (max (or highest-score 0) score))))))

(defn- update-lowest-score [activities path statement]
  (update-in activities (conj path :lowest-score)
             (fn [lowest-score]
               (let [score (xapi/get-score statement)]
                 (if (nil? score)
                   lowest-score
                   (min (or lowest-score 0) score))))))


(defn by-activity
  ([]
   {})
  ([activities statement]
   (let [path [(-> statement :object :id)]]
     (-> activities
       (update-description path statement)
       (update-course path statement)
       (update-attempts path statement)
       (update-completions path statement)
       (update-successes path statement)
       (update-highest-score path statement)
       (update-lowest-score path statement)))))


(defn by-activity-actor
  ([]
   {})
  ([activities statement]
   (let [path [(xapi/actor statement) (-> statement :object :id)]]
     (-> activities
       (update-description path statement)
       (update-course path statement)
       (update-attempts path statement)
       (update-completions path statement)
       (update-first-completion-date path statement)
       (update-completion-date path statement)
       (update-successes path statement)
       (update-first-success-date path statement)
       (update-success-date path statement)
       (update-highest-score path statement)
       (update-lowest-score path statement)))))
