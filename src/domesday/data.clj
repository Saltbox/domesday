(ns domesday.data
  (:require [clojure.core.async :as async :refer [<! go chan sub pub]]
            [domesday.xapi :as xapi])
  (:import [org.joda.time Period]))


(defn- process-statements [statements-channel processor-fn]
  "Process all the statements from the async channel using the
   processor function, yielding the result in the returned channel."
  ; A processor function should take either two arguments or zero.
  ;  If two arguments, they are the accumulated data and the next
  ;   Experience API statement.
  ;  If zero arguments, the processor function should return an initial
  ;   data structor that it will accept as its first argument.
  (async/reduce processor-fn (processor-fn) statements-channel))


(defn- by-group-name [groups statement]
  (if (empty? groups)
    :all
    (let [actor (:actor statement)
          [group-name _] (first (filter (fn [[group-name agents]]
                           (some (partial xapi/same-agent? actor) agents))
                                        groups))]
      group-name)))


(defn- process-group [group-pub-ch processor-fn group-name]
  "Process an agent group, yielding the result in the returned channel."
  (let [group-ch (chan)]
    (sub group-pub-ch group-name group-ch)
    (process-statements group-ch processor-fn)))


(defn gather-agents [statement-ch]
  "Merge all statement actors into a single sequence of agents, yielding
   the sequence in the returned channel."
  (go
    (loop [agents {}]
      (if-let [statement (<! statement-ch)]
        (recur (update-in agents [(xapi/actor statement)]
                          (fn [agent]
                            (if (and (not (:name agent)) (:name (:actor statement)))
                              (:actor statement)
                              agent))))
        agents))))



(defn tabulate [statement-ch groups processors]
  ; groups is a map from group-name to a list of agents
  ; processors is a map from processor-name to processor function
  ; do things
  ; yield per-group output of processors in the returned channel
  (let [group-pub-ch (pub statement-ch (partial by-group-name groups))
        groups (or (keys groups) [:all])]
    (async/into {} (async/merge (map (fn [[processor-name processor-fn]]
                      (go [processor-name
                       (<! (async/into {}
                             (async/merge (map (fn [group-name]
                                                 (go
                                                   [group-name
                                                    (<! (process-group
                                                          group-pub-ch processor-fn group-name))]))
                                               groups))))]))
                    processors)))))


(defn count-agents
  ([]
     0)
  ([acc statement]
     (inc acc)))

(defn- update-completions [activities statement]
   (if (xapi/completed-activity? statement)
     (update-in activities [(-> statement :object :id) (xapi/actor statement) :completions] #(if %1 (inc %1) 1))
     activities))

(defn- update-successes [activities statement]
   (if (xapi/successful-activity? statement)
     (update-in activities [(-> statement :object :id) (xapi/actor statement) :successes] #(if %1 (inc %1) 1))
     activities))


(defn- add-duration
  ([]
   0)
  ([milliseconds iso8601-duration]
   (+ milliseconds (.getMillis (.toStandardDuration (Period/parse iso8601-duration))))))


(defn- update-durations [activities statement]
  (if-let [duration (-> statement :result :duration)]
    (update-in activities [(-> statement :object :id) (xapi/actor statement) :total-duration] #(if-not (nil? %1) (add-duration %1 (-> statement :result :duration)) (add-duration)))
    activities))


(defn- save-score [activities statement]
  (if-let [score (-> statement :result :score)]
    (let [timestamp (:timestamp statement)]
      (update-in activities [(-> statement :object :id) (xapi/actor statement) :scores] #(conj (or % []) [score timestamp])))
    activities))


(defn completed-activities
  ([]
     {})
  ([activities statement]
   (-> activities
     (update-completions statement)
     (update-successes statement)
     (save-score statement)
     (update-durations statement))))
