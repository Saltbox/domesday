(ns domesday.utils
  (:require [org.httpkit.client :as http-kit]
            [clojure.core.async :refer [put! chan]]))


(defn http
  ([options]
     (http (chan) options))
  ([ch options]
     (http-kit/request options (fn [response]
                                 (put! ch response)))
     ch))
