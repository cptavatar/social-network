(ns crawler.blocks
  (require [clojure.core.async :as async]
           [clj-time.core :as ct]
           [clj-time.coerce :as ctc]
           [twit.twitter-core :as to]
           [clojure.tools.logging :refer (debug info error)]
           [util.assertion :refer :all]))

; This module is for the common building blocks used to put together
; our twitter processing queues

;; We should try to chain smaller, specialized functions to build up
;; up our functionality - for instance, have a series of functions that
;; operate on the twitter response and that take in a function & work-unit

(defn onto-chan
  "Our own form of onto-chan solving two issues, we don't want
  to close the queue (saves an arg) and more importanly, we want
  to know if the chan was closed or not"
  [chan-name seq]
  (if (empty? seq)
    true
    (reduce #(and %1 %2)
            (map (fn [x]
                   (async/>!! chan-name x))
                 seq))))


(defn read-q
  "Create a closure over a channel for reading
  when used with def it can make it easier to pass readers"
  [q]
  (async/<!! q))

(defn chunker
  "Given a size and a function that generates content,
  loop over that content until you have a built up a chunk
  of the desired size then return"
  [size queue-reader]
  {:pre [(pos? size) (not-nil? queue-reader)]}
  (loop [prev []]
    (let [n (queue-reader) x (conj prev n)]
      (cond
        (= size (count x))
        x
        (nil? n)
        (when (> (count prev) 0)
          prev)
        :default
        (recur x)))))


(defn wait
  "Pause thread execution for a given number of millis"
  [x]
  {:pre [(pos? x)]}
  (Thread/sleep x))

(defn rate-limit
  "Given a twitter result, look at the headers. If we are out of calls
  wait until we can go again - of course, twitter's result is based on
  seconds since the epoch so we have some conversion to do"
  [result work-unit]
  (if-not (pos? (Integer/parseInt (:x-rate-limit-remaining (:headers result))))
    (let [rate-limit-seconds (Long/parseLong (:x-rate-limit-reset (:headers result)))
          current-seconds (long (/ (ctc/to-long (ct/now)) 1000))
          ; wait a min of 30 sec, but no more than 15m in case of weird results
          time-to-wait (min 900 (max 30 (- rate-limit-seconds current-seconds)))]
      (debug "Rate limiting in effect, twitter says" rate-limit-seconds
             "we say its currently " current-seconds
             "so we are going to wait " time-to-wait "seconds")
      (wait (* time-to-wait 1000))))
  result)


(defn page-results
  "Take a f that generates a twitter response, work unit for f
  look at the response headers. while there are another page of reuslts
  keep calling f with the same work unit and updated page parameters"
  [f work-unit]
  (loop [wk work-unit]
    (let [response (f wk) cursor (:next_cursor_str (:body response))]
      (info "processing cursor" cursor)
      (if (= "0" cursor)
        response
        (recur (merge work-unit {:cursor cursor}))))))

(defn safe-reader [reader]
  (try
    (reader)
    (catch Exception e
      (do
        (error e "Error with " reader ": " (.getMessage e))
        nil))))


(defn thread-loop
  "Given a function that reads from (hopefully) a queue and another that consumes from it
  loop until we get nil which means the queue is shut down"
  [queue-reader queue-consumer]
  (async/thread
    (loop [n (safe-reader queue-reader)]
      (if (nil? n)
        (do
          (error "found nil, exiting thread.")
          nil)
        (do
          (try (queue-consumer n)
               (catch Exception e (error e "Error with " queue-consumer ": " (.getMessage e))))
          (recur (queue-reader)))))))

(defn inject-timestamp
  "Given a map v, inject a key called :timestap with the current time in millis
  then call f (v)"
  [f v]
  (f (merge v {:timestamp (ctc/to-long (ct/now))})))

(defn process-chain
  "Given a `generator` that takes work and produces results,
  a vector of functions `chain`, and a unit of work,
  link the results of previous function to the input of the next, giving
  all functions in the chain a peek at the work unit if they need it
   "
  [generator chain work]
  (loop [result (generator work) fun-list chain]
    (if (empty? fun-list)
      result
      (recur
        ((first fun-list) result work)
        (rest fun-list)))))

(defn to-str
  "Convenience function to make sure something is a string"
  [x]
  (if (or (nil? x)
          (string? x))
    x
    (str x)))



(defn multi-loop
  "Launch n thread loops where x is the reader and y is the consumer"
  [n x y]
  (dotimes [a n] (thread-loop x y)))

(defn load-queue [q-name fn-name]
  (let [results (fn-name)]
    (debug "Found " (count results) "using" fn-name "to load on queue" q-name)
    (if (nil? results)
      true
      (onto-chan q-name
                 (map
                   #(hash-map :data %1)
                   results)))))

(defn shutdown-watch
  "Our simple process control for CLI - Block on input,when we get anything, shutdown queues. "
  [chan-vec]
  (println "press enter to stop processing")
  (read-line)
  (map async/close! chan-vec))