Compatibility kit and testbed for evaluating resilience of [reactive-kafka](https://github.com/akka/reactive-kafka)-based streams. The kit tests specific failure scenarios and recovery of streams in the presence of availability issues with Kafka/ZooKeeper at various stages of stream life-cycle and failures in the streams themselves. 

The kit will talk to you nicely, f.e.:

<pre>
A stream with KafkaActorSubscriber-based Sink
 - should be resilient to Kafka being unavailable initially and begin processing as soon as Kafka becomes available
   + Given Kafka and ZooKeeper are unavailable
   + When the stream is created
   + And the stream is run
   + Then Kafka producer will retry and fail to send a message
   + And KafkaActorSubscriber backing the Sink is restarted
   + When Kafka and ZooKeeper become available
   + Then the stream should start processing and producing messages onto output topic
...
</pre>

The project is self-contained by means of [scalatest-embedded-kafka](https://github.com/manub/scalatest-embedded-kafka). To build and execute all tests:
<pre>
sbt test
</pre>
