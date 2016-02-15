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
  "com.softwaremill.reactivekafka" %% "reactive-kafka-core" % "0.8.4" exclude ("junit", "junit"),
  "com.typesafe.akka" %% "akka-actor" % "2.3.14",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.1.0",
  "com.typesafe.akka" %% "akka-stream-testkit-experimental" % "2.0" % "test",
  "net.manub" %% "scalatest-embedded-kafka" % "0.4.3" % "test"
)

publishTo := Some("Crowdmix Artifactory" at "https://artifactory.dev.crwd.mx/artifactory/libs-release-local")

parallelExecution in Test := false