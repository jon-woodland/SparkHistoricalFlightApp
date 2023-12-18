import sbt._

object Dependencies {
  lazy val scalaTest = Seq(
    "org.scalatest" %% "scalatest" % "3.2.13" % Test,
    "org.scalatestplus" %% "scalacheck-1-15" % "3.2.9.0" % Test
  )

  val circeVersion = "0.13.0"
  val pureconfigVersion = "0.15.0"
  val catsVersion = "2.2.0"
  val sparkVersion = "3.2.1"

  lazy val core = Seq(
    // cats FP libary
    "org.typelevel" %% "cats-core" % catsVersion,

    // support for JSON formats
    "io.circe" %% "circe-core" % circeVersion,
    "io.circe" %% "circe-generic" % circeVersion,
    "io.circe" %% "circe-parser" % circeVersion,
    "io.circe" %% "circe-literal" % circeVersion,

    // support for typesafe configuration
    "com.github.pureconfig" %% "pureconfig" % pureconfigVersion,

    // parallel collections
    // "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",

    // spark
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided, // for submiting spark app as a job to cluster
    // "org.apache.spark" %% "spark-sql" % sparkVersion, // for simple standalone spark app

    // algebra
    "com.twitter" %% "algebird-core" % "0.13.9",

    // kafka streams
    "org.apache.kafka" %% "kafka-streams-scala" % "3.4.0",
    "com.goyeau" %% "kafka-streams-circe" % "0.6.3",

    // logging
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    // hadoop
    "org.apache.hadoop" % "hadoop-aws" % "3.3.1"
  )
}
