(ns domesday.data
  (:require [clojure.core.async :as async :refer [<! go chan sub pub]]
            [domesday.xapi :as xapi]))


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


(defn completed-activities
  ([]
     {})
  ([activities statement]
     activities))
