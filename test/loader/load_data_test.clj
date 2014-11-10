(ns loader.load-data-test
  (:require [loader.load-data :as undertest]
            [crawler.store-node :as sn]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [crawler.store-node :as store-node]))

(deftest valid-profile-tests
  (testing "we let valid nodes pass"
    (is  (undertest/valid-profile? {:profileName "foo"})))
  (testing "we reject invalid nodes"
    (is (not (undertest/valid-profile? {}))))
  )

;; Try matching up results / nodes
;; - make sure results gets returned
;; - make sure node gets id added, put on queue
(deftest enqueue-nodes-tests
  (testing "that we parse twitter results properly, collating, putting on next queue ")
  ;; set up our test data
  (let [results {:body [{:screen_name   "Beta" :id_str "2" :statuses_count 3
                         :friends_count 4 :followers_count 5}]}
        nodes [{:profileName "bEta"} {:profileName "alpha"}]
        fakeQueue (async/chan 1)]
    (with-redefs [store-node/store-node #(async/>!! fakeQueue %)]

                 (let [output (undertest/enqueue-nodes results nodes)
                       {profileName   :profileName
                        profileId     :profileId
                        statusCount   :statusCount
                        friendCount   :friendCount
                        followerCount :followerCount
                        type          :type} (async/<!! fakeQueue)]

                   (is (= output results))
                   (is (= profileName "Beta"))
                   (is (= profileId "2"))
                   (is (= statusCount 3))
                   (is (= friendCount 4))
                   (is (= followerCount 5))
                   (is (= type "dealer"))
                   (async/close! fakeQueue)
                   (is (nil? (async/<!! fakeQueue)))))))


(run-tests)
