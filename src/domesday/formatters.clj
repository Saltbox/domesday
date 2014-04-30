(ns domesday.formatters)


(defn- dump-csv [headers rows]
  ; TODO
  (str headers "\n" rows))


(defn- completed-activities-row
  [result agent]
  ; TODO
  result)


(defn completed-activities
  [results agents]
  ; TODO csv header names
  (dump-csv ["" "" ""]
    (loop [rows []
           [result & more] (seq results)]
      (if result
        (recur
          (conj rows
                (completed-activities-row result agents))
          more)
        rows))))
