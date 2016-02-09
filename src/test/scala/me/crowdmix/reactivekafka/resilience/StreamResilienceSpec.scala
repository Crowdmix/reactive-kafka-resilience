package me.crowdmix.reactivekafka.resilience

import java.util.UUID.randomUUID

import akka.actor.ActorSystem
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, OverflowStrategy, Supervision}
import akka.testkit.TestKit
import com.softwaremill.react.kafka.KafkaMessages.KafkaMessage
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import kafka.serializer.{StringDecoder, StringEncoder}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest._
import org.scalatest.concurrent.Eventually

import scala.reflect.io.Directory
import scala.util.control.NonFatal


class StreamResilienceSpec extends TestKit(ActorSystem("StreamResilienceSpec", ConfigFactory.parseString(
  """
    |akka {
    |  log-config-on-start = off
    |
    |  loggers = ["akka.testkit.TestEventListener"]
    |  loglevel = "DEBUG"
    |  stdout-loglevel = "DEBUG"
    |
    |  logger-startup-timeout = 10s
    |  jvm-exit-on-fatal-error = off
    |
    |  log-dead-letters = on
    |  log-dead-letters-during-shutdown = on
    |
    |  actor {
    |    debug {
    |      autoreceive = on
    |      receive = on
    |      lifecycle = on
    |      fsm = on
    |      event-stream = on
    |      unhandled = on
    |    }
    |  }
    |}
  """.stripMargin)))
  with FlatSpecLike
  with Matchers
  with EmbeddedKafka
  with Eventually
  with BeforeAndAfterAll {

  implicit val kafkaConfig = EmbeddedKafkaConfig(kafkaPort = 9092, zooKeeperPort = 2181)
  val brokerList = s"localhost:${kafkaConfig.kafkaPort}"
  val zooKeeperHost = s"localhost:${kafkaConfig.zooKeeperPort}"

  val alwaysResume: Supervision.Decider = {
    case NonFatal(ex) =>
      println(s"Exception while processing: '${ex.getMessage}'", ex)
      Supervision.Resume
  }

  implicit val materializer =
    ActorMaterializer(
      ActorMaterializerSettings(system)
        .withSupervisionStrategy(alwaysResume)
    )(system)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A stream with KafkaActorSubscriber-based Sink" should
    "be resilient to Kafka becoming unavailable and continue processing when Kafka comes back up" in {
    val kafka = new ReactiveKafka()

    try {
      val zkDir = testZooKeeperDir()
      val kafkaDir = testKafkaDir()
      EmbeddedKafka.startZooKeeper(zkDir)
      EmbeddedKafka.startKafka(kafkaDir)
      // let ZooKeeper/Kafka start up
      //TODO bake this into EmbeddedKafka itself, fe via heartbeating of some sort
      Thread.sleep(5000)

      val topic = testTopic()
      val clientId = testClientId()

      val producerProperties = ProducerProperties(
        brokerList = brokerList,
        topic = topic,
        clientId = clientId,
        encoder = new StringEncoder)

      val kafkaActorSubscriber = ActorSubscriber[String](
        system.actorOf(
          kafka.producerActorProps(producerProperties),
          testSinkName()))

      val kafkaSink = Sink(kafkaActorSubscriber)

      val sourceActorRef = Source.actorRef(bufferSize = 100, overflowStrategy = OverflowStrategy.fail)
        .map { v: String =>
          v.toUpperCase
        }
        .to(kafkaSink)
        .run()

      sourceActorRef ! "a"

      eventually {
        consumeFirstStringMessageFrom(topic) shouldBe "A"
      }

      EmbeddedKafka.stopKafka()

      sourceActorRef ! "b"

      Thread.sleep(3000)  // let Reactive Kafka producer give up retrying before bringing Kafka back up
      //TODO ascertain [akka://WriteOnlyStreamSpec/user/kafka-sink-UUID] Failed to send messages after X tries.
      //TODO ascertain [akka://WriteOnlyStreamSpec/user/kafka-sink-UUID] restarted

      EmbeddedKafka.startKafka(kafkaDir)
      Thread.sleep(5000)  // let Kafka start up

      sourceActorRef ! "c"

      eventually {
        consumeFirstStringMessageFrom(topic) shouldBe "C"
      }
    } finally {
      EmbeddedKafka.stopKafka()
      EmbeddedKafka.stopZooKeeper()
    }
  }

  it should "be resilient to Kafka being unavailable initially and begin processing as soon as Kafka becomes available" in {
    val kafka = new ReactiveKafka()

    val topic = testTopic()

    val producerProperties = ProducerProperties(
      brokerList = brokerList,
      topic = topic,
      clientId = testClientId(),
      encoder = new StringEncoder)

    val kafkaActorSubscriber = ActorSubscriber[String](
      system.actorOf(
        kafka.producerActorProps(producerProperties),
        testSinkName()))

    val kafkaSink = Sink(kafkaActorSubscriber)

    Source.repeat("a")
      .map { v: String =>
        v.toUpperCase
      }
      .to(kafkaSink)
      .run()

    Thread.sleep(5000)  // let Reactive Kafka producer give up retrying before starting Kafka
    //TODO ascertain [akka://WriteOnlyStreamSpec/user/kafka-sink-UUID] Failed to send messages after X tries.
    //TODO ascertain [akka://WriteOnlyStreamSpec/user/kafka-sink-UUID] restarted

    withRunningKafka {
      eventually {
        consumeFirstStringMessageFrom(topic) shouldBe "A"
      }
    }
  }

  "A stream with KafkaActorPublisher-based Source" should
    "be resilient to Kafka being unavailable initially and begin processing as soon as Kafka becomes available" in {
    val zkDir = testZooKeeperDir()
    val kafkaDir = testKafkaDir()
    try {
      EmbeddedKafka.startZooKeeper(zkDir)
      EmbeddedKafka.startKafka(kafkaDir)

      // let ZooKeeper/Kafka start up - otherwise publishStringMessageToKafka() may throw net.manub.embeddedkafka.KafkaUnavailableException
      Thread.sleep(10 * 1000)

      val topic = testTopic()

      publishStringMessageToKafka(topic, "a")
      publishStringMessageToKafka(topic, "b")

      EmbeddedKafka.stopKafka()

      val kafka = new ReactiveKafka()

      val consumerProperties =
        ConsumerProperties(
          brokerList = brokerList,
          zooKeeperHost = zooKeeperHost,
          topic = topic,
          groupId = testConsumerGroupId(),
          decoder = new StringDecoder())
      val consumerActor = system.actorOf(
        kafka.consumerActorProps(consumerProperties),
        testSourceName())

      val sinkProbe =
        Source(ActorPublisher[KafkaMessage[String]](consumerActor))
          .map(_.message)
          .map { v: String =>
            v.toUpperCase
          }
          .toMat(TestSink.probe[String])(Keep.right)
          .run()

      sinkProbe.request(n = 2)

      Thread.sleep(60 * 1000)  // ample time for things to fail if they are meant to

      EmbeddedKafka.startKafka(kafkaDir)
      Thread.sleep(5000)  // let Kafka start up

      sinkProbe.expectNext("A", "B")
    } finally {
      EmbeddedKafka.stopKafka()
      EmbeddedKafka.stopZooKeeper()
    }
  }

  def testClientId() = s"clientId-${randomUUID()}"
  def testConsumerGroupId() = s"groupId-${randomUUID()}"
  def testTopic() = s"topic-${randomUUID()}"
  def testSourceName() = s"kafka-source-${randomUUID()}"
  def testSinkName() = s"kafka-sink-${randomUUID()}"

  def testZooKeeperDir() = Directory.makeTemp("zookeeper")
  def testKafkaDir() = Directory.makeTemp("kafka")
}
