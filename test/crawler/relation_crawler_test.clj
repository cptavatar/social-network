(ns crawler.relation-crawler-test
  (:require [crawler.relation-crawler :as undertest]
            [crawler.store-node :as sn]
            [crawler.update-node :as update-node]
            [clojure.core.async :as async]
            [clojure.test :refer :all]
            [util.workunit :as wu]))


(deftest pick-next-test
  "That we prefer the fast lane over the slow"
  (let [fast (async/chan 1) slow (async/chan 1)]
    (async/>!! fast "fast")
    (async/>!! slow "slow")
    (is (= "fast" (undertest/pick-next fast slow)))
    (is (= "slow" (undertest/pick-next fast slow)))))

(deftest lookup-relations-callback-test
  "Check that we add the data to the queue, removing the callback"
  (let [fakeQueue (async/chan 1)
        testData (wu/merge-metadata
                   (wu/make-unit {:test "unit"})
                   wu/meta-callback "foo")]
    (with-redefs [undertest/lookup-relations-queue fakeQueue]
                 (undertest/lookup-relations-callback testData)
                 (let [queueRecord (async/<!! fakeQueue)]
                   (is (= "unit" (:test (wu/unit-data queueRecord))))
                   (is (not (wu/unit-metadata-contains? queueRecord wu/meta-callback)))))))

(deftest ensure-profile-exists-test
  (testing "That if all is well we add callback and add to queue"
    (let [fakeQueue (async/chan 1)
          testData (wu/make-unit {:profileId "profileId"})]
      (with-redefs [update-node/update-primary-node #(async/>!! fakeQueue %)]
                   (undertest/ensure-profile-exists testData)
                   (let [queueRecord (async/<!! fakeQueue)]
                     (is (= (wu/unit-metadata wu/meta-callback queueRecord) undertest/lookup-relations-callback))
                     (is (= (:profileId (wu/unit-data queueRecord)) "profileId"))))))

  (testing "That if we are missing data we explode..."
    (let [fakeQueue (async/timeout 500)
          testData (wu/make-unit {:badData "profileId"})]
      (with-redefs [update-node/update-primary-node #(async/>!! fakeQueue %)]
                   (undertest/ensure-profile-exists testData))
      (is (nil? (async/<!! fakeQueue)))))
  )

(deftest propagate-test
  (testing "that if our counts are small, we hop in the fast lane"
    (let [fakeFriendQ (async/chan 1)
          fakeFollowerQ (async/chan 1)
          testData (wu/make-unit {:badData "profileId"})]
      (with-redefs [undertest/friend-lookup-queue fakeFriendQ
                    undertest/follower-lookup-queue fakeFollowerQ]
                   (undertest/propagate testData))
      ; (is (nil? (async/<!! fakeQueue)))))
      ))
  (testing "that if we are missing data or if we have too many friends/followers we go to the slow lane.")
  )

(deftest enqueue-node-test

  )

(run-tests)
