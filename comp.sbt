name := "Impressions"
version := "1.0"
scalaVersion := "2.10.6"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.4.1" % "provided",
  //"org.json4s"  %% "json4s-native" % "3.3.0"
  "org.json4s"  %% "json4s-jackson" % "3.3.0"
)


