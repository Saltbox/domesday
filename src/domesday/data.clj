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
  (async/reduce processor-fn (processor-fn) statements-channel))


(defn- by-group-name [groups statement]
  (let [actor (:actor statement)
        matches-agent? (partial xapi/same-agent? actor)
        group-names (into #{} (map first (filter (fn [[_ agents]]
                                                   (some
                                                     matches-agent?
                                                     agents))
                                                 groups)))]
    (if (empty? group-names)
      #{:catch-all :all}
      (conj group-names :all))))

(defn- close-group-channels [group-chs]
  (doseq [ch (vals group-chs)]
      (async/close! ch)))

(defn- setup-group-channels
  "Create a new source and mult channel for each group."
  [groups]
  (let [source-chs (into {} (map #(vec [% (chan)]) (keys groups)))]
    (debug "Constructed" (count source-chs) "channels for groups")
    [(into {} (map #(vec [% (async/mult (source-chs %))]) (keys groups)))
     source-chs]))

(defn- dispatch-group-channels
  "This function dispatches statements to all matching group channels."
  [statement-ch groups source-chs]
  ; Dispatch statements to all appropriate group channels while
  ; statement channel is open.
  (go-loop []
    (if-let [statement (<! statement-ch)]
      (do
        ; call (by-group-name groups statement) which returns a sequence of
        ; group names
        ; doseq over return, >! return-val statement
        (doseq [group-name (by-group-name groups statement)]
          (when-let [ch (source-chs group-name)]
            (>! ch statement)))
        (recur))

      ; No futher statements, close all the channels
      (do
        (debug "Finished dispatching statements to groups")
        (close-group-channels source-chs)))))


(defn- process-group [tap-chs report report-name group-name]
  (let [processor-fn (r/make-processor report)
        ch (tap-chs [group-name report-name])]
    (go
      (let [result (<! (process-statements ch processor-fn))]
        (debug "Finished getting result for process-group for" group-name)
        [group-name result])))
  )


(defn- async-fan-in [dest f coll]
  ; f is a function that returns a channel yielding a value.
  (async/into dest (async/merge (map f coll))))


(defn tabulate
  [statement-ch groups reports]
  ; groups is a map from group-name to a list of agents
  ; reports is a map from report-name to report definition
  ; yields per-group output of reports in the returned channel
  (debug "Entering tabulate")
  (let [[mult-chs source-chs] (setup-group-channels groups)
        tap-chs (into {} (for [group (keys groups)
                               report (keys reports)]
                           (let [ch (chan)]
                             [[group report] (async/tap (group mult-chs) ch)])))
        result-ch
        (async-fan-in {}
                      (fn [[report-name report]]
                        (debug "Processing for" report-name)
                        (go
                          [report-name
                           (<! (async-fan-in {} (partial process-group
                                                         tap-chs report
                                                         report-name)
                                             (keys groups)))]))
                      reports)]
    (dispatch-group-channels statement-ch groups source-chs)
    result-ch))


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
