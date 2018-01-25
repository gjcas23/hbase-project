name := "hbase-project"

version := "1.0"

scalaVersion := "2.11.7"


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "org.apache.spark" %% "spark-hive" % "2.1.0",
  "org.apache.spark" %% "spark-streaming" % "2.1.0",
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.0",
  "org.apache.kafka" %% "kafka" % "0.10.0.1",
  "org.apache.hbase" % "hbase-client" % "1.2.6",
  "org.apache.hbase" % "hbase-common" % "1.2.6",
  "org.apache.hbase" % "hbase-server" % "1.2.6",
  "com.typesafe" % "config" % "1.2.1"
)