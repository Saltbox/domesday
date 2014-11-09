(ns domesday.data
  (:require [taoensso.timbre :as timbre]
            [clojure.core.async :as async :refer [<! >! go chan sub pub go-loop]]
            [domesday.data.reports :as r]
            [domesday.xapi :as xapi]))

(timbre/refer-timbre)


(defn- process-statements
  "Process all the statements from the async channel using the
   processor function, yielding the result in the returned channel."
  [statements-channel processor-fn]
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


(defn- dispatch-group-channels
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


(defn- process-group [report [group-name ch]]
  (let [per-process-group-ch (chan)
        processor-fn (r/make-processor report)]
    (async/tap ch per-process-group-ch)
    (go
      [group-name (<! (process-statements per-process-group-ch processor-fn))])))


(defn- async-map-map [f coll]
  (async/into {} (async/merge (map f coll))))


(defn tabulate [statement-ch groups reports]
  ; groups is a map from group-name to a list of agents
  ; processors is a map from processor-name to processor function
  ; do things
  ; yield per-group output of processors in the returned channel
  (debug "Entering tabulate")
  (let [group-chs (dispatch-group-channels statement-ch groups)
        groups (or (keys groups) [:all])]
    (debug "Constructed group channels" group-chs)
    (async-map-map
      (fn [[report-name report]]
        (debug "Processing for" report-name)
        (go
          [report-name
           (<! (async-map-map (partial process-group report) group-chs))]))
      reports)))


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
