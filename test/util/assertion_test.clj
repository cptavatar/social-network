(ns util.assertion-test
  (:require [util.assertion :as undertest]
            [clojure.test :refer :all]))


(deftest test-all-not-nil
  (let [test {:a 2 :b "b" :c []}]
    (is (= (undertest/all-not-nil (:a test))))
    (is (= false (undertest/all-not-nil (:x test))))
    (is (= true (undertest/all-not-nil (:a test) (:b test))))
    (is (= false (undertest/all-not-nil (:b test) (:y test))))
    (is (= false (undertest/all-not-nil (:a test) (:b test) (:x test))))
    (is (= true (undertest/all-not-nil (:a test) (:b test) (:c test))))))
