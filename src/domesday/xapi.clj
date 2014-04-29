(ns domesday.xapi
  (:refer-clojure :exclude [resolve])
  (:require [domesday.utils :refer [http]]
            [taoensso.timbre :as timbre]
            [clojure.data.json :as json]
            [clojurewerkz.urly.core :refer [url-like resolve]]
            [clojure.core.async :refer [go chan <! onto-chan close!]]))


(timbre/refer-timbre)


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
             (onto-chan ch (:statements body) false)
             (when (:more body)
               (debug "Found more statements")
               (recur (str (resolve endpoint-url (:more body))))))))
      
       (debug "Finished fetching statements")
       (close! ch))

     ch))
