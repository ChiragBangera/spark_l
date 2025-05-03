// Name of the package
name := "scala_test"
// Version of our package
version := "1.0"
// Version of Scala
scalaVersion := "2.12.18"
// Spark library dependencies
libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "3.0.0-preview2",
"org.apache.spark" %% "spark-sql" % "3.0.0-preview2"
)

// Enable JVM forking and set Java options to fix Java 17 accessibility issues
fork in run := true

javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
)