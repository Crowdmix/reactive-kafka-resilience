name := "reactive-kafka-resilience"

organization := "me.crowdmix"

scalaVersion := "2.11.7"

credentials += Credentials(
  "Artifactory Realm", "artifactory.dev.crwd.mx",
  sys.env.getOrElse("ARTIFACTORY_REPOSITORY_USER_NAME", "administrator"),
  sys.env.getOrElse("ARTIFACTORY_REPOSITORY_USER_PASSWORD", "not-the-real-password")
)

resolvers ++= Seq(
  "Artifactory Realm" at "https://artifactory.dev.crwd.mx/artifactory/libs-release"
)

libraryDependencies ++= Seq(
  "com.softwaremill.reactivekafka" %% "reactive-kafka-core" % "0.8.5" exclude ("junit", "junit"),
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.typesafe.akka" %% "akka-testkit" % "2.4.2" % "test",
  "com.typesafe.akka" %% "akka-stream-testkit" % "2.4.2" % "test",
  "net.manub" %% "scalatest-embedded-kafka" % "0.4.3" % "test",
  "me.crowdmix" %% "event-model-scala" % "128"
)

publishTo := Some("Crowdmix Artifactory" at "https://artifactory.dev.crwd.mx/artifactory/libs-release-local")

parallelExecution in Test := false