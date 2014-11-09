(ns domesday.data.queries
  (:require [domesday.xapi :as xapi]
            [clojure.core.match :refer [match]]))

; Utilities
(defn- annotate-statement [take as by statement]
  (update-in statement [:context :extensions :_result as]
             (fn [{existing-value :value}]
               {:value (by existing-value (take statement))
                :by by})))

(defn query [{:keys [take as by]}]
  (fn [step]
    (fn [result statement]
      (step result (annotate-statement take as by statement)))))

(defn- bnil [f]
  (fn [a b]
    (match [a b]
           [nil b] b
           [a nil] a
           [nil nil] nil
           [a b] (f a b))))

(defn- string-or [a b]
  (or (when (pos? (.length a)) a)
      (when (pos? (.length b)) b)))

(defn- take-greater [a b]
  (if (pos? (compare a b))
    a
    b))

(defn- take-lesser [a b]
  (if (neg? (compare a b))
    a
    b))

; Queries
(def highest-score
  (query {:take   xapi/get-score
          :as     :highest-score
          :by     (bnil max)}))

(def lowest-score
  (query {:take   xapi/get-score
          :as     :lowest-score
          :by     (bnil min)}))

(def description
  (query {:take (fn [statement]
                  (or (-> statement :object :definition :name xapi/get-lang)
                      (-> statement :object :id)))
          :as   :name
          :by   (bnil string-or)}))

(def course
  (query {:take (fn [statement]
                  (let [parent (xapi/context-parent statement)]
                    (or (some-> parent :definition :name xapi/get-lang)
                        (some-> parent :id))))
          :as   :course
          :by   (bnil string-or)}))

(def attempts
  (query {:take (fn [_] 1)
          :as :attempts
          :by (bnil +)}))

(def completions
  (query {:take (fn [statement] (when (xapi/completed-activity? statement) 1))
          :as :completions
          :by (bnil +)}))

(def successes
  (query {:take (fn [statement] (when (xapi/successful-activity? statement) 1))
          :as :successes
          :by (bnil +)}))

(def failures
  (query {:take (fn [statement] (when (xapi/not-successful-activity? statement) 1))
          :as :failures
          :by (bnil +)}))

(def completion-date
  (query {:take (fn [statement] (when (xapi/completed-activity? statement)
                                  (:timestamp statement)))
          :as :completion-date
          :by (bnil take-greater)}))

(def first-completion-date
  (query {:take (fn [statement] (when (xapi/completed-activity? statement)
                                  (:timestamp statement)))
          :as :first-completion-date
          :by (bnil take-lesser)}))

(def success-date
  (query {:take (fn [statement] (when (xapi/successful-activity? statement)
                                  (:timestamp statement)))
          :as :success-date
          :by (bnil take-greater)}))

(def first-success-date
  (query {:take (fn [statement] (when (xapi/successful-activity? statement)
                                  (:timestamp statement)))
          :as :first-success-date
          :by (bnil take-lesser)}))