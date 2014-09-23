(ns domesday.xapi
  (:refer-clojure :exclude [resolve])
  (:require [domesday.utils :refer [http]]
            [domesday.protocols :refer [StatementsSource]]
            [clojurewerkz.urly.core :refer [url-like]]
            [taoensso.timbre :as timbre]
            [cheshire.core :refer [parse-string]]
            [clojure.string :refer [split join]]
            [clojurewerkz.urly.core :refer [resolve]]
            [clojure.core.async :refer [go go-loop chan <! onto-chan close!]])
  (:import [domesday.protocols xAPIEndpoint]))


(timbre/refer-timbre)


(def ifi-keys #{:mbox :mbox_sha1sum :openid :account})

; TODO test me
(defn same-agent? [x y]
  (every? #(= (x %1) (y %1)) ifi-keys))


(defn get-score [statement]
  (if-let [scaled (-> statement :result :score :scaled)]
    scaled
    (let [raw (-> statement :result :score :raw)
          ; (or raw 0) because raw may be nil, but we need to
          ; detect that after we determine mins, so that raw can
          ; be set to mins.
          mins (or (-> statement :result :score :min)
                   (min 0 (or raw 0)))
          maxs (or (-> statement :result :score :max)
                   (max mins (or raw 0)))]
      (if (nil? raw)
        nil
        (if (zero? maxs)
          0
          (/ (- raw mins) (- maxs mins)))))))


(defn get-lang
  ([lang-map]
   (get-lang "en-US" lang-map))
  ([lang lang-map]
   (or
     (get lang-map lang)
     (get lang-map (first (keys lang-map))))))



(defn completed-activity? [statement]
  (-> statement
    :result
    :completion))


(defn successful-activity? [statement]
  (-> statement
    :result
    :success))


; TODO test me
(defn actor [statement]
  (into {}
    [(first (filter (fn [[k v]]
                     (contains? ifi-keys k))
                   (:actor statement)))]))




(defn fetch-statements
  [ch endpoint-url auth]
  (debug "Starting statement fetch from" (str endpoint-url))
  (go-loop [url endpoint-url
            total 0]
      (debug "Sending statement request")
      (let [{status :status
             body :body
             error-message :error} (<! (http {:method :get
                                    :url url
                                    :basic-auth auth
                                    :headers {"X-Experience-API-Version" "1.0.0"}}))
            body (try (parse-string body true) (catch Exception e nil))]
        (debug "Got statement response")
        (if (and (= 200 status) body)
          (do
            (debug
              "Received" (count (:statements body)) "statements."
              total "total fetched so far.")
            (onto-chan ch (:statements body) (nil? (:more body)))
            (when-let [more (:more body)]
              (let [next-url (str (resolve endpoint-url more))]
                (debug "Found more statements at" next-url)
                (recur next-url (+ total (count (:statements body)))))))
          
          (do
            (error
              "Got bad response from server: " status "status. Retrying.")
            (debug "Error response:" error-message)
            (recur endpoint-url total)))))
   
    (debug "Finished fetching statements")

  ch)


(extend xAPIEndpoint
  StatementsSource
  {:fetch (fn [this ch since until]
            (let [url (-> this
                          :url
                          url-like
                          (.mutateQuery
                            (join "&"
                                  (-> (split (:query this) #"&")
                                    (conj (str "since=" since))
                                    (conj (str "until=" until)))))
                          str)]
              (fetch-statements
                ch
                url
                [(:user this) (:password this)])))})
