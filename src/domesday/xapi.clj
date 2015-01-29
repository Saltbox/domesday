(ns domesday.xapi
  (:refer-clojure :exclude [resolve])
  (:require [domesday.utils :refer [http]]
            [domesday.protocols :refer [StatementsSource]]
            [clojurewerkz.urly.core :refer [url-like]]
            [taoensso.timbre :as timbre]
            [cheshire.core :refer [parse-string]]
            [clojure.string :refer [split join]]
            [clojurewerkz.urly.core :refer [resolve]]
            [clojure.core.async :refer [go go-loop chan <! timeout onto-chan
                                        close!]])
  (:import [domesday.protocols xAPIEndpoint]))

(timbre/refer-timbre)

(def ^:dynamic *retry-delay* 1000)                          ; in milliseconds

(def ifi-keys #{:mbox :mbox_sha1sum :openid :account})

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
      (when-not (nil? raw)
        (if (zero? maxs)
          0
          (/ (- raw mins) (- maxs mins)))))))

(defn get-lang
  ([lang-map]
   (get-lang :en-US lang-map))
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

(defn not-successful-activity? [statement]
  (-> statement
    :result
    (:success true)
    not))

(defn actor [statement]
  (into {}
    [(first (filter (fn [[k _]]
                     (contains? ifi-keys k))
                   (:actor statement)))]))

(defn context-parent [statement]
  (some-> statement
          :context
          :contextActivities
          :parent
          (get 0)))

(defn- process-response
  [{status :status
    body :body
    error-message :error}]
  (info "Got statement response" status)
  (when error-message
    (error "Got error from server" error-message))
  (condp contains? status
    #{200} (try
             [:ok (parse-string body true)]
             (catch Exception e
               (error e "Failed to parse response")
               [:error nil]))

    #{502 503 504} [:retry nil]

    [:error nil]))

(defn- process-statements [[status {statements :statements
                                    more :more}] ch url]
  ; if error, close ch and yield [nil 0]
  ; if retry, wait some, then yield original url and 0
  ; else, yield [next-url (count statements)]
  (go
    (condp = status
      :error (do
               (close! ch)
               [nil 0])
      :retry (do
               (<! (timeout *retry-delay*))
               [url 0])
      :ok (do
            (onto-chan ch statements (nil? more))
            [(when more (str (resolve url more))) (count statements)])
      )))

(defn fetch-statements
  [ch endpoint-url auth]
  (debug "Starting statement fetch from" endpoint-url)
  (go-loop [url endpoint-url
            total 0]
      (let [params {:method :get
                    :url url
                    :basic-auth auth
                    :headers {"X-Experience-API-Version" "1.0.0"}}
            [next-url statement-count] (-> params
                                           http
                                           <!
                                           process-response
                                           (process-statements ch url)
                                           <!)]
        (when (pos? statement-count)
          (debug
            "Received" statement-count "statements."
            (+ statement-count total) "total fetched so far."))

        (when next-url
          (recur next-url (+ total statement-count)))))

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
                [(:username this) (:password this)])))})
