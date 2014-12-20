name := "res/pagerank"

version := "1.0"

scalaVersion := "2.11.4"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0",
  "org.apache.spark" %% "spark-core" % "1.2.0"
)