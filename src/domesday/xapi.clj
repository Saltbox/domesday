(ns domesday.xapi
  (:refer-clojure :exclude [resolve])
  (:require [domesday.utils :refer [http]]
            [taoensso.timbre :as timbre]
            [clojure.data.json :as json]
            [clojurewerkz.urly.core :refer [url-like resolve]]
            [clojure.core.async :refer [go chan <! onto-chan close!]]))


(timbre/refer-timbre)


; TODO test me
(defn same-agent? [x y]
  (let [ifi-keys [:mbox :mbox_sha1sum :openid :account]]
    (every? #(= (x %1) (y %1)) ifi-keys)))


(defn fetch-statements
  ([endpoint-url auth]
     (fetch-statements (chan) endpoint-url auth))
  ([ch endpoint-url auth]
     (debug "Starting statement fetch from" (str endpoint-url))
     (go
       (loop [url endpoint-url]
         (let [{status :status
                body :body} (<! (http {:method :get
                                       :url url
                                       :basic-auth auth
                                       :headers {"X-Experience-API-Version" "1.0.0"}}))
               body (try (json/read-str body :key-fn keyword) (catch Exception e nil))]
           (when (and (= 200 status) body)
             (debug "Received" (count (:statements body)) "statements")
             (onto-chan ch (:statements body) (nil? (:more body)))
             (debug "Found more statements")
             (when (:more body)
               (recur (str (resolve endpoint-url (:more body))))))))
      
       (debug "Finished fetching statements"))

     ch))
