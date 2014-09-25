(ns domesday.protocols)


(defprotocol StatementsSource
  (fetch [this ch since until] "Fetches the xAPI statements from this endpoint."))


(defrecord xAPIEndpoint [url username password query])
