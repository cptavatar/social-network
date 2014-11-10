(ns util.edn-helper
    (require
        [clojure.edn :as edn]))

; Utilities to help loading up data from edn files

(defn- edn-row-processor
    "Process a single row in an EDN file recursively until we get to the end"
    [reader results row-processor]
    (let [x (edn/read {:eof nil} reader)]
        (if (nil? x)
            results
            (edn-row-processor reader (conj results (row-processor x)) row-processor))))

(defn load-from-edn
    "For a given filename and row processing function, return a collection based on the results"
    ([filename row-processor]
     (with-open [r (java.io.PushbackReader. (clojure.java.io/reader filename))]
         (edn-row-processor r [] row-processor)))
    ([filename]
     (load-from-edn filename (fn [x] x))))
