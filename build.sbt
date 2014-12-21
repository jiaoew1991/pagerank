name := "pagerank"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "2.6.0",
  "org.apache.hadoop" % "hadoop-client" % "2.6.0",
  ("org.apache.spark" %% "spark-core" % "1.2.0").
      exclude("org.eclipse.jetty.orbit", "javax.servlet").
      exclude("org.eclipse.jetty.orbit", "javax.transaction").
      exclude("org.eclipse.jetty.orbit", "javax.mail").
      exclude("org.eclipse.jetty.orbit", "javax.activation").
      exclude("commons-beanutils", "commons-beanutils-core").
      exclude("commons-collections", "commons-collections").
      exclude("commons-collections", "commons-collections").
      exclude("com.esotericsoftware.minlog", "minlog")
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("org", "eclipse", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
