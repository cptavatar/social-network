(ns crawler.workflow)

(defprotocol workflow
  (load-data [this])
  (start [this])
  )
