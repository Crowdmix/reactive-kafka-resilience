package me.crowdmix.reactivekafka.resilience

import java.util.UUID.randomUUID

import akka.actor.ActorSystem
import akka.stream.actor.ActorSubscriber
import akka.stream.scaladsl.{Source, Sink}
import akka.stream.{OverflowStrategy, ActorMaterializerSettings, ActorMaterializer, Supervision}
import akka.testkit.TestKit
import com.softwaremill.react.kafka.{ProducerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringEncoder
import net.manub.embeddedkafka.{EmbeddedKafkaConfig, EmbeddedKafka}
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
  val kafkaBrokerList = s"localhost:${kafkaConfig.kafkaPort}"

  val topic = "testTopic"
  val clientId = "testClientId"

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

  val producerProperties = ProducerProperties(
    brokerList = kafkaBrokerList,
    topic = topic,
    clientId = clientId,
    encoder = new StringEncoder)

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A stream with KafkaActorSubscriber-based Sink" should
    "be resilient to Kafka becoming unavailable and continue processing when Kafka comes back up" in {
    val kafka = new ReactiveKafka()

    try {
      val zkLogDir = Directory.makeTemp("zookeeper")
      val kafkaLogDir = Directory.makeTemp("kafka")
      EmbeddedKafka.startZooKeeper(zkLogDir)
      EmbeddedKafka.startKafka(kafkaLogDir)
      // let ZooKeeper/Kafka start up
      //TODO bake this into EmbeddedKafka itself, fe via heartbeating of some sort
      Thread.sleep(3000)

      val kafkaActorSubscriber = ActorSubscriber[String](
        system.actorOf(
          kafka.producerActorProps(producerProperties),
          s"kafka-sink-$randomUUID"))

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

      EmbeddedKafka.startKafka(kafkaLogDir)
      Thread.sleep(3000)  // let Kafka start up

      sourceActorRef ! "c"

      eventually {
        consumeFirstStringMessageFrom(topic) shouldBe "C"
      }
    } finally {
      EmbeddedKafka.stopKafka()
      EmbeddedKafka.stopZooKeeper()
    }
  }

  it should "be resilient to Kafka not being available initially and begin processing as soon as Kafka becomes available" in {
    val kafka = new ReactiveKafka()

    val kafkaActorSubscriber = ActorSubscriber[String](
      system.actorOf(
        kafka.producerActorProps(producerProperties),
        s"kafka-sink-$randomUUID"))

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
}
