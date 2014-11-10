(ns twit.twitter-oauth
  (:require
    [twitter.oauth :refer :all]
    [util.edn-helper :as edn-helper]))

;; Easiest way to generate tokens I've found:
;; Twitter4j sample code, standalone single class - just make sure to use same key/secret
;; http://twitter4j.org/en/code-examples.html


;; Create a file called conf/edn/twitter-keys.edn that looks like...
;; { :consumer {:key myAppKey :secret myAppSecret} :clients [{:key myWorkerKey :secret myWorkerSecret}]}
;; add as many clients as you want...

(def tokens (first(edn-helper/load-from-edn "conf/edn/twitter-keys.edn")))
(def all-tokens (into [] (map (fn [x] (make-oauth-creds (:key (:consumer tokens))
                                                        (:secret (:consumer tokens))
                                                        (:key x)
                                                        (:secret x)))
                              (:clients tokens))))
