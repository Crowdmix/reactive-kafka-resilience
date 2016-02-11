package me.crowdmix.reactivekafka.resilience

import java.util.UUID.randomUUID

import akka.actor._
import akka.event.Logging.LogEvent
import akka.pattern._
import akka.stream.actor.{ActorPublisher, ActorSubscriber}
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, OverflowStrategy, Supervision}
import akka.testkit.TestKit
import akka.util.Timeout
import com.softwaremill.react.kafka.KafkaMessages.KafkaMessage
import com.softwaremill.react.kafka.{ConsumerProperties, ProducerProperties, ReactiveKafka}
import com.typesafe.config.ConfigFactory
import kafka.serializer.{StringDecoder, StringEncoder}
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest._
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.{Microseconds, Seconds, Span}

import scala.concurrent.duration._
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
    |
    |    custom {
    |      dispatchers {
    |        bounded-fork-join-dispatcher {
    |          type = Dispatcher
    |          executor = "fork-join-executor"
    |          mailbox-requirement = "akka.dispatch.BoundedMessageQueueSemantics"
    |        }
    |      }
    |    }
    |  }
    |}
  """.stripMargin)))
  with FlatSpecLike
  with Matchers
  with EmbeddedKafka
  with Eventually
  with ScalaFutures
  with BeforeAndAfter
  with BeforeAndAfterAll {

  implicit val kafkaConfig = EmbeddedKafkaConfig()
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

  val BoundedForkJoinDispatcher = "akka.actor.custom.dispatchers.bounded-fork-join-dispatcher"

  after {
    EmbeddedKafka.stopKafka()
    EmbeddedKafka.stopZooKeeper()
  }

  override implicit val patienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A stream with KafkaActorSubscriber-based Sink" should
    "be resilient to Kafka becoming unavailable and continue processing when Kafka comes back up" in {
    val kafka = new ReactiveKafka()

    val zkDir = testZooKeeperDir()
    val kafkaDir = testKafkaDir()
    EmbeddedKafka.startZooKeeper(zkDir)
    EmbeddedKafka.startKafka(kafkaDir)
    // let ZooKeeper/Kafka start up
    //TODO bake this into EmbeddedKafka itself, fe via heartbeating of some sort
    Thread.sleep(10 * 1000)

    val topic = testTopic()
    val clientId = testClientId()

    val producerProperties = ProducerProperties(
      brokerList = brokerList,
      topic = topic,
      clientId = clientId,
      encoder = new StringEncoder)

    val kafkaActorSubscriber = ActorSubscriber[String](
      system.actorOf(
        kafka.producerActorProps(producerProperties).withDispatcher(BoundedForkJoinDispatcher),
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

    expectLogEvent(_.message == "Failed to send messages after 3 tries.") {  // let Reactive Kafka producer give up retrying before starting Kafka
      sourceActorRef ! "b"
    }

    //TODO ascertain [akka://StreamResilienceSpec/user/kafka-sink-UUID] restarted

    EmbeddedKafka.startKafka(kafkaDir)
    Thread.sleep(5 * 1000)  // let Kafka start up

    sourceActorRef ! "c"

    eventually {
      consumeFirstStringMessageFrom(topic) shouldBe "C"
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
        kafka.producerActorProps(producerProperties).withDispatcher(BoundedForkJoinDispatcher),
        testSinkName()))

    val kafkaSink = Sink(kafkaActorSubscriber)

    Source.repeat("a")
      .map { v: String =>
        v.toUpperCase
      }
      .to(kafkaSink)
      .run()

    // let Reactive Kafka producer give up retrying before starting Kafka
    expectLogEvent(_.message == "Failed to send messages after 3 tries.")()

    //TODO ascertain [akka://StreamResilienceSpec/user/kafka-sink-UUID] restarted

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
    EmbeddedKafka.startZooKeeper(zkDir)
    EmbeddedKafka.startKafka(kafkaDir)

    // let ZooKeeper/Kafka start up - otherwise publishStringMessageToKafka() may throw net.manub.embeddedkafka.KafkaUnavailableException
    Thread.sleep(5 * 1000)

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
      kafka.consumerActorProps(consumerProperties).withDispatcher(BoundedForkJoinDispatcher),
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
    Thread.sleep(5 * 1000)  // let Kafka start up

    sinkProbe.expectNext("A", "B")
  }

  it should
    "be resilient to Kafka becoming unavailable during processing and resume processing as soon as Kafka becomes available" in {
    val zkDir = testZooKeeperDir()
    val kafkaDir = testKafkaDir()
    EmbeddedKafka.startZooKeeper(zkDir)
    EmbeddedKafka.startKafka(kafkaDir)

    // let ZooKeeper/Kafka start up - otherwise publishStringMessageToKafka() may throw net.manub.embeddedkafka.KafkaUnavailableException
    Thread.sleep(5 * 1000)

    val topic = testTopic()

    publishStringMessageToKafka(topic, "a")
    publishStringMessageToKafka(topic, "b")
    publishStringMessageToKafka(topic, "c")

    val kafka = new ReactiveKafka()

    val consumerProperties =
      ConsumerProperties(
        brokerList = brokerList,
        zooKeeperHost = zooKeeperHost,
        topic = topic,
        groupId = testConsumerGroupId(),
        decoder = new StringDecoder())
    val consumerActor = system.actorOf(
      kafka.consumerActorProps(consumerProperties).withDispatcher(BoundedForkJoinDispatcher),
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
    sinkProbe.expectNext("A", "B")

    EmbeddedKafka.stopKafka()

    sinkProbe.request(n = 1)
    Thread.sleep(60 * 1000)  // ample time for things to fail if they are meant to

    EmbeddedKafka.startKafka(kafkaDir)
    Thread.sleep(5 * 1000)  // let Kafka start up

    sinkProbe.expectNext("C")
  }

  "A stream with KafkaActorPublisher-based Source and KafkaActorSubscriber-based Sink" should
    "be resilient to failure with exception in intermediate processing stage" in withRunningKafka {
    val inputTopic = testTopic()

    val publishInput = (msg: String) => publishStringMessageToKafka(inputTopic, msg)

    Seq("a", "b", "c", "d", "e") foreach publishInput

    val kafka = new ReactiveKafka()

    val consumerProperties =
      ConsumerProperties(
        brokerList = brokerList,
        zooKeeperHost = zooKeeperHost,
        topic = inputTopic,
        groupId = testConsumerGroupId(),
        decoder = new StringDecoder())
    val consumerActor = system.actorOf(
      kafka.consumerActorProps(consumerProperties).withDispatcher(BoundedForkJoinDispatcher),
      testSourceName())

    val clientId = testClientId()

    val outputTopic = testTopic()

    val producerProperties = ProducerProperties(
      brokerList = brokerList,
      topic = outputTopic,
      clientId = clientId,
      encoder = new StringEncoder)

    val kafkaActorSubscriber = ActorSubscriber[String](
      system.actorOf(
        kafka.producerActorProps(producerProperties).withDispatcher(BoundedForkJoinDispatcher),
        testSinkName()))

    Source(ActorPublisher[KafkaMessage[String]](consumerActor))
      .map(_.message)
      .map {
        case "c" =>
          throw new RuntimeException("Intermediate stage primed to fail with exception")
        case v: String =>
          v.toUpperCase
      }
      .to(Sink(kafkaActorSubscriber))
      .run()

    def expectOutput(msg: String) = eventually {
      consumeFirstStringMessageFrom(outputTopic) shouldBe msg
    }

    Seq("A", "B", "D", "E") foreach expectOutput
  }

  def expectLogEvent(condition: LogEvent => Boolean)(block: => Unit) = {
    val logListener = system.actorOf(Props(new Actor {
      var capturedEvent: Option[LogEvent] = None

      def receive = {
        case event: LogEvent if condition(event) =>
          capturedEvent = Some(event)
        case "report" =>
          sender() ! capturedEvent
        case _ =>
      }
    }))
    system.eventStream.subscribe(logListener, classOf[LogEvent])

    try {
      block

      eventually {
        whenReady(logListener.ask("report")(Timeout(5.seconds)).mapTo[Option[LogEvent]]) {
          _ shouldBe 'defined
        }
      }
    } finally {
      system.eventStream.unsubscribe(logListener)
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
