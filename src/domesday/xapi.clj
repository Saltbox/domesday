(ns domesday.xapi
  (:refer-clojure :exclude [resolve])
  (:require [domesday.utils :refer [http]]
            [taoensso.timbre :as timbre]
            [cheshire.core :refer :all]
            [clojurewerkz.urly.core :refer [resolve]]
            [clojure.core.async :refer [go go-loop chan <! onto-chan close!]]))


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
  ([endpoint-url auth]
     (fetch-statements (chan) endpoint-url auth))
  ([ch endpoint-url auth]
     (debug "Starting statement fetch from" (str endpoint-url))
     (go-loop [url endpoint-url]
         (let [{status :status
                body :body} (<! (http {:method :get
                                       :url url
                                       :basic-auth auth
                                       :headers {"X-Experience-API-Version" "1.0.0"}}))
               body (try (parse-string body true) (catch Exception e nil))]
           (when (and (= 200 status) body)
             (debug "Received" (count (:statements body)) "statements")
             (onto-chan ch (:statements body) (nil? (:more body)))
             (when (:more body)
               (debug "Found more statements")
               (recur (str (resolve endpoint-url (:more body))))))))
      
       (debug "Finished fetching statements")

     ch))
