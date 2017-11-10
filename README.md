# aws-mq

A simple approach to messaging using AWS SNS and SQS.

## Installation



## Usage

```clojure
(ns mq
  (require [aws-mq.core :as mq]))
  
;; Create topic and queue and a routing (subscription) from topic to queue
(mq/route! "my_topic" "my_queue")


;; Start consuming messages from `my_queue`
(mq/consume! "my_queue" #(println "Received:" %))


;; Publish to `my_topic`
(mq/publish! "my_topic" "Hello world!")

=> Received: {:headers nil :body "Hello world!"}

(mq/publish! "my_topic" "Such body!" "Many subject" {:wow true})

=> Received: {:headers {:wow true :subject "Many subject"} :body "Such body!"}
```

## License

Copyright © 2017 Lauri Siltanen

Distributed under the Eclipse Public License, the same as Clojure.
