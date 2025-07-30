name := "spark-app"

version := "0.1"

scalaVersion := "2.13.13"

val sparkVersion = "4.0.0"
val deltaVersion = "3.2.0" // latest as of July 2025
val kafkaVersion = "3.5.0" // latest stable widely supported

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",

  // Delta Lake
  "io.delta" %% "delta-spark" % deltaVersion,

  // Spark Kafka integration
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.0",
  "org.apache.spark" %% "spark-token-provider-kafka-0-10" % "3.5.0",

  // Kafka client (optional but sometimes required explicitly)
  "org.apache.kafka" % "kafka-clients" % kafkaVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
)