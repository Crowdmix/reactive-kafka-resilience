package me.crowdmix.reactivekafka.resilience

import java.util.UUID.randomUUID

import akka.actor._
import akka.event.Logging
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
import org.scalatest.time.{Seconds, Span}

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
  with GivenWhenThen
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

    Given("Kafka and ZooKeeper are available")
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
    val kafkaProducer = system.actorOf(
      kafka.producerActorProps(producerProperties).withDispatcher(BoundedForkJoinDispatcher),
      testSinkName())
    val kafkaActorSubscriber = ActorSubscriber[String](kafkaProducer)

    val kafkaSink = Sink(kafkaActorSubscriber)

    And("the stream is created and run")
    val sourceActorRef = Source.actorRef(bufferSize = 100, overflowStrategy = OverflowStrategy.fail)
      .map { v: String =>
        v.toUpperCase
      }
      .to(kafkaSink)
      .run()

    sourceActorRef ! "a"

    And("the stream is processing and outputting messages onto a topic")
    eventually {
      consumeFirstStringMessageFrom(topic) shouldBe "A"
    }

    When("Kafka becomes unavailable")
    EmbeddedKafka.stopKafka()

    Then("Kafka producer will retry and fail to send a message")
    And("KafkaActorSubscriber backing the Sink is restarted")
    expectLogEvents(
      List(
        KafkaProducerRetriesAndFailsToSend,
        kafkaActorSubscriberRestarted(kafkaProducer)))
    {
      sourceActorRef ! "b"
    }

    When("Kafka comes back up")
    EmbeddedKafka.startKafka(kafkaDir)
    Thread.sleep(5 * 1000)  // let Kafka start up

    sourceActorRef ! "c"

    Then("the stream should resume processing and outputting messages onto the topic")
    eventually {
      consumeFirstStringMessageFrom(topic) shouldBe "C"
    }
  }

  it should "be resilient to Kafka being unavailable initially and begin processing as soon as Kafka becomes available" in {
    Given("Kafka and ZooKeeper are unavailable")
    val kafka = new ReactiveKafka()

    val topic = testTopic()

    val producerProperties = ProducerProperties(
      brokerList = brokerList,
      topic = topic,
      clientId = testClientId(),
      encoder = new StringEncoder)
    val kafkaProducer = system.actorOf(
      kafka.producerActorProps(producerProperties).withDispatcher(BoundedForkJoinDispatcher),
      testSinkName())
    val kafkaActorSubscriber = ActorSubscriber[String](kafkaProducer)

    val kafkaSink = Sink(kafkaActorSubscriber)

    When("the stream is created")
    val stream =
      Source.repeat("a")
        .map { v: String =>
          v.toUpperCase
        }
        .to(kafkaSink)

    And("the stream is run")
    Then("Kafka producer will retry and fail to send a message")
    And("KafkaActorSubscriber backing the Sink is restarted")
    expectLogEvents(
      List(
        KafkaProducerRetriesAndFailsToSend,
        kafkaActorSubscriberRestarted(kafkaProducer)))
    {
      stream.run()
    }

    When("Kafka and ZooKeeper become available")
    withRunningKafka {
      Then("the stream should start processing and producing messages onto output topic")
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

    Given("an input topic with messages")
    publishStringMessageToKafka(topic, "a")
    publishStringMessageToKafka(topic, "b")

    And("Kafka is unavailable and ZooKeeper is available")
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

    And("the stream is created and run")
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

    When("Kafka becomes available")
    EmbeddedKafka.startKafka(kafkaDir)
    Thread.sleep(5 * 1000)  // let Kafka start up

    Then("the stream will start consuming and processing messages from the input topic")
    sinkProbe.expectNext("A", "B")
  }

  it should
    "be resilient to Kafka becoming unavailable during processing and resume processing as soon as Kafka becomes available" in {
    Given("Kafka and ZooKeeper are available")
    val zkDir = testZooKeeperDir()
    val kafkaDir = testKafkaDir()
    EmbeddedKafka.startZooKeeper(zkDir)
    EmbeddedKafka.startKafka(kafkaDir)

    // let ZooKeeper/Kafka start up - otherwise publishStringMessageToKafka() may throw net.manub.embeddedkafka.KafkaUnavailableException
    Thread.sleep(5 * 1000)

    val topic = testTopic()

    And("an input topic contains messages")
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

    When("the stream is created and run")
    val sinkProbe =
      Source(ActorPublisher[KafkaMessage[String]](consumerActor))
        .map(_.message)
        .map { v: String =>
          v.toUpperCase
        }
        .toMat(TestSink.probe[String])(Keep.right)
        .run()

    Then("the stream will start consuming and processing messages from the input topic")
    sinkProbe.request(n = 2)
    sinkProbe.expectNext("A", "B")

    When("Kafka becomes unavailable")
    EmbeddedKafka.stopKafka()

    sinkProbe.request(n = 1)
    Thread.sleep(60 * 1000)  // ample time for things to fail if they are meant to

    And("Kafka later becomes available")
    EmbeddedKafka.startKafka(kafkaDir)
    Thread.sleep(5 * 1000)  // let Kafka start up

    Then("the stream should resume consuming and processing messages from the input topic")
    sinkProbe.expectNext("C")
  }

  "A stream with KafkaActorPublisher-based Source and KafkaActorSubscriber-based Sink" should
    "be resilient to failure with exception in intermediate processing stage" in withRunningKafka {
    Given("Kafka and ZooKeeper are available")
    val inputTopic = testTopic()

    val publishInput = (msg: String) => publishStringMessageToKafka(inputTopic, msg)

    And("an input topic contains messages")
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

    And("the stream is created and run")
    When("the stream throws exception in intermediate processing stage")
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

    Then("the stream should drop message causing exception and continue consuming and processing messages " +
      "from the input topic and producing them onto the output topic")
    Seq("A", "B", "D", "E") foreach expectOutput
  }

  type LogEventCondition = LogEvent => Boolean
  def expectLogEvent(condition: LogEventCondition)(block: => Unit): Unit = expectLogEvents(List(condition))(block)
  def expectLogEvents(conditions: List[LogEventCondition])(block: => Unit): Unit = {
    object ReportRemaining

    val logListener = system.actorOf(Props(new Actor {
      var remaining: List[LogEventCondition] = conditions

      def receive = {
        case event: LogEvent =>
          remaining match {
            case Nil =>
            case condition :: tail =>
              if (condition(event))
                remaining = tail
          }
        case ReportRemaining =>
          sender() ! remaining
        case _ =>
      }
    }))
    system.eventStream.subscribe(logListener, classOf[LogEvent])

    try {
      block

      eventually {
        whenReady(logListener.ask(ReportRemaining)(Timeout(5.seconds)).mapTo[List[LogEventCondition]]) {
          _ shouldBe Nil
        }
      }
    } finally {
      system.eventStream.unsubscribe(logListener)
    }
  }
  val KafkaProducerRetriesAndFailsToSend: LogEventCondition = { event =>
    event.isInstanceOf[Logging.Error] &&
      event.asInstanceOf[Logging.Error].cause.getClass.getCanonicalName == "kafka.common.FailedToSendMessageException" &&  // matching on canonical name avoids headaches of maintaining precise dependency on Kafka
      event.message == "Failed to send messages after 3 tries."
  }
  def kafkaActorSubscriberRestarted(kafkaProducer: ActorRef): LogEventCondition = { event =>
    event.logSource == kafkaProducer.path.toString &&
      event.message == "restarted"
  }

  def testClientId() = s"clientId-${randomUUID()}"
  def testConsumerGroupId() = s"groupId-${randomUUID()}"
  def testTopic() = s"topic-${randomUUID()}"
  def testSourceName() = s"kafka-source-${randomUUID()}"
  def testSinkName() = s"kafka-sink-${randomUUID()}"

  def testZooKeeperDir() = Directory.makeTemp("zookeeper")
  def testKafkaDir() = Directory.makeTemp("kafka")
}
