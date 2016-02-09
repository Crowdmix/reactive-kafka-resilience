package me.crowdmix.reactivekafka.resilience

import akka.actor.ActorSystem
import akka.testkit.TestKit
import com.typesafe.config.ConfigFactory
import net.manub.embeddedkafka.{EmbeddedKafkaConfig, EmbeddedKafka}
import org.scalatest._
import org.scalatest.concurrent.Eventually


class WriteOnlyStreamSpec extends TestKit(ActorSystem("WriteOnlyStreamSpec", ConfigFactory.parseString(
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

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  "A write-only stream" should "TODO" in withRunningKafka {
  }
}
