(ns crawler.blocks-test
  (:require [crawler.blocks :as undertest]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [clj-time.core :as ct]
            [clj-time.coerce :as ctc]))



(deftest chunker-tests
  "Tests the verify we can break up items into chunks"
  (let [chunk-test-one (async/chan)
        chunk-test-two (async/chan)
        chunk-test-three (async/chan)]
    ; load some data on to our test channels for testing
    (async/onto-chan chunk-test-one (vector 1 2 3))
    (async/onto-chan chunk-test-two (vector 1 2 3 4 5 6))
    (async/onto-chan chunk-test-three (vector))
    ; if get less than chunk size should come back in one chunk
    (is (= [1 2 3] (undertest/chunker 5 #(async/<!! chunk-test-one))))
    ; if we have more than chunk size we should come back in 2 chunks
    (is (= [[1 2 3 4 5], [6]]
           (vector
             (undertest/chunker 5 #(async/<!! chunk-test-two))
             (undertest/chunker 5 #(async/<!! chunk-test-two)))))
    ; if channel is empty/done, we are done
    (is (= nil (undertest/chunker 5 #(async/<!! chunk-test-three))))))

(deftest thread-loop-tests
  "Tests running producer/consumer in separate thread"
  (let [loop-test-one (async/chan) loop-test-two (async/chan)]
    (async/onto-chan loop-test-one (vector 1 2 3))
    (is (= [1 2 3] (do
                     (undertest/thread-loop
                       #(async/<!! loop-test-one)
                       #(async/>!! loop-test-two %1))
                     (undertest/chunker 3
                                        #(async/<!! loop-test-two)))))))




(deftest rate-limit-test
  "Tests checking for rate limit header, waiting"
  (def rate-limit-result (atom 0))
  (with-redefs
    [undertest/wait
     (fn sleep [duration]
       (swap! rate-limit-result (fn [x] duration)))]
    (undertest/rate-limit
      {:headers {:x-rate-limit-remaining "0"
                 :x-rate-limit-reset     (str (long (/ (+ 10000 (ctc/to-long (ct/now))) 1000)))}}
      nil
      ))
  (is (>= @rate-limit-result 30000))
  (is (< @rate-limit-result 90000)))

;; Tests on (tw-profile-pager ...)
;; not done
(deftest page-results-test
  (let [result (undertest/page-results
                 (fn [x]
                   (if (contains? x :cursor)
                     {:body {:next_cursor_str "0"}}
                     {:body {:next_cursor_str "2"}})) {})]
    (is (= {:body {:next_cursor_str "0"}} result))))


(deftest process-chain-test
  (let [generator (fn [x] (str "a" x))
        f1 (fn [x y] (str "b" x y))
        f2 (fn [x y] (str "c" x y))]
    (is (= "a3" (generator 3)))
    (is (= "ba33" (f1 (generator 3) 3)))
    (is (= "cba333" (f2 (f1 (generator 3) 3) 3)))
    (is (= "a3" (undertest/process-chain generator [] 3)))
    (is (= "ba33" (undertest/process-chain generator [f1] 3)))
    (is (= "cba333" (undertest/process-chain generator [f1 f2] 3)))
    )
  )

(deftest onto-chan-test
  (is (undertest/onto-chan 'foo '()))
  (with-redefs
    [async/>!! (fn [x y] (and (= x 'test-chan') (<= 0 y 5)))]
    (is (undertest/onto-chan 'test-chan' [0 1 2 3 4]))
    (is (not (undertest/onto-chan 'test-chan' [0 1 2 3 6]))))
  )

(deftest read-q-test

  )

(deftest inject-timestamp-test
  (let [date (undertest/inject-timestamp :timestamp {})]
    (is (not (nil? date))))
  )

(deftest to-str-test
  (= "2" (undertest/to-str 2))
  (= "3" (undertest/to-str "3")))

(deftest multi-loop-test)

(run-tests)
