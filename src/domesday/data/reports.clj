(ns domesday.data.reports
  (:require [domesday.data.queries :as q]
            [domesday.xapi :as xapi]))

;; Sample reports

; Here's a really simple report, which counts the number of statements in
; the group it processes.
(def count-statements
  {:query q/any
   :key (fn [_] (vector :count))
   :init {}})

(def by-activity
  {:query (comp q/description
                q/course
                q/attempts
                q/completions
                q/successes
                q/failures
                q/highest-score
                q/lowest-score)
   :key #(vector (-> % :object :id))
   :init  {}})

(def by-activity-actor
  {:query (comp q/description
                q/course
                q/attempts
                q/completions
                q/first-completion-date
                q/completion-date
                q/successes
                q/failures
                q/first-success-date
                q/success-date
                q/highest-score
                q/lowest-score)
   :key #(vector (xapi/actor %1) (-> %1 :object :id))
   :init {}})


;; Utilities
(defn- merge-statement-annotations-to-accumulator
  [acc [result & results]]
  (if-let [[k {v :value by :by}] result]
    (recur
      (assoc acc k
             (by (k acc) v))
      results)
    acc))

(defn- annotate-key [key-fn]
  (fn [step]
    (fn [result statement]
      (step result (assoc-in statement [:context :extensions :_result :key]
                             (key-fn statement))))))

(defn- extract-annotations []
  (fn [step]
    (fn [result statement]
      (step result (get-in statement [:context :extensions :_result])))))

(defn- step-fn [results annotations]
  (update-in results (:key annotations)
             (fn [results-by-key]
               (merge-statement-annotations-to-accumulator
                 (or results-by-key {})
                 (seq (dissoc annotations :key))))))

(defn make-processor [report]
  (let [xf (comp (:query report) (annotate-key (:key report))
                 (extract-annotations))
        query (xf step-fn)]
    (fn
      ([]
       (:init report))
      ([acc v]
       (query acc v)))))