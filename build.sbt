name := "DynamicPathFileSink"

version := "0.1"

scalaVersion := "2.12.2"

val flinkVersion = "1.9.2"

libraryDependencies ++= Seq(
  "org.apache.flink" % "flink-core" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-java" % flinkVersion,
)