(ns aws-mq.core
  (:require [amazonica.aws.sns :as sns]
            [amazonica.aws.sqs :as sqs]
            [clojure.string :as string]
            [clojure.core.async :as async]
            [cheshire.core :as json])
  (:import (java.util Map)
           (clojure.lang IFn)))

;; Private
(def ^:private queue-consumers (atom []))

(defn- list-topics []
  (->> (sns/list-topics)
       :topics
       (map :topic-arn)))

(defn- topic-arn [name]
  (->> (list-topics)
       (map (fn [x] {:name (last (string/split x #":"))
                     :arn  x}))
       (filter #(= name (:name %)))
       (first)
       :arn))

(defn- queue-url [queue]
  (sqs/find-queue queue))

(defn- topic-queue-write-policy [topic-arn queue-arn]
  (json/generate-string {:Statement [{:Effect    "Allow"
                                      :Principal "*"
                                      :Action    "SQS:SendMessage"
                                      :Resource  queue-arn
                                      :Condition {:ForAllValues:ArnEquals {:aws:SourceArn topic-arn}}}]}))

;; Public

(defn create-topic!
  "Creates a new topic with the given name if it does not exist. Always returns the topic arn."
  [^String name]
  (:topic-arn (sns/create-topic :name name)))

(defn create-queue!
  "Creates a new queue with the given name if it does not exist. Always returns the queue-url."
  [^String name]
  (:queue-url (sqs/create-queue name)))

(defn route!
  "Route messages from topic to queue. Creates topic and queue if they do not exist."
  [^String topic-name ^String queue-name]
  (create-queue! queue-name)
  (let [topic-arn (create-topic! topic-name)
        queue-arn (sqs/arn queue-name)]
    (sqs/set-queue-attributes queue-name {"Policy" (topic-queue-write-policy topic-arn queue-arn)})
    (sns/subscribe :protocol "sqs"
                   :topic-arn topic-arn
                   :endpoint queue-arn)))

(defn publish!
  "Send message to the given topic."
  [^String topic-name
   ^String message
   & [^String subject
      ^Map headers]]
  (sns/publish (cond-> {:topic-arn (topic-arn topic-name)
                        :message   message}
                       subject (assoc :subject subject)
                       headers (assoc :message-attributes headers))))

(defn consume!
  "Consumes messages from queue.
   Each message is processed by the provided handler function.
   Each message is a map with the following signature:
   `{:headers map
     :body message-as-string}`"
  [^String queue-name
   ^IFn handler-fn
   & [{:keys [max-messages
              auto-ack?]
       :or   {max-messages 10
              auto-ack?    false}}]]
  (let [channel            (async/chan)
        poison-channel     (async/chan)
        queue-url          (queue-url queue-name)
        fetch-fn           #(sqs/receive-message :queue-url queue-url
                                                 :wait-time-seconds 20
                                                 :max-number-of-messages max-messages
                                                 :delete auto-ack?
                                                 :attribute-names ["All"])
        consumer           (async/go (while true
                                       (when-let [messages (-> (async/<! channel)
                                                               :messages
                                                               (not-empty))]
                                         (println "BBBBBBBBBBB" messages)
                                         (doseq [{:keys [body] :as message} messages]
                                           (let [body (json/parse-string body true)
                                                 {:keys [Subject Message MessageAttributes]} body]
                                             (handler-fn {:headers (assoc MessageAttributes :subject Subject)
                                                          :body    Message})
                                             (when-not auto-ack? (sqs/delete-message (assoc message :queue-url queue-url))))))))
        consumer-reference {:channel        channel
                            :poison-channel poison-channel}]
    (swap! queue-consumers conj consumer-reference)
    (async/go-loop [messages (fetch-fn)]
      (println "AAAAAAAAAAa" messages)
      (async/go (async/>! channel messages))
      (when (async/alt! poison-channel false
                        :default true)
        (recur (fetch-fn))))
    consumer-reference))

(defn stop-consumer! [{:keys [channel poison-channel] :as consumer}]
  (async/put! poison-channel "STOP")
  (async/close! channel)
  (async/close! poison-channel)
  (println "consumers before remove: " @queue-consumers)
  (swap! queue-consumers remove #(= consumer %))
  (println "consumers after remove: " @queue-consumers))

(defn stop-consumers! []
  (run! stop-consumer! @queue-consumers)
  (reset! queue-consumers []))
