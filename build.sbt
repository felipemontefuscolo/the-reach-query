name := "Impressions"
version := "1.0"
scalaVersion := "2.10.4"
//scalaVersion := "2.9.2"

resolvers ++= Seq(
  "anormcypher" at "http://repo.anormcypher.org/",
  "Mandubian repository snapshots" at "https://github.com/mandubian/mandubian-mvn/raw/master/snapshots/",
  "Mandubian repository releases" at "https://github.com/mandubian/mandubian-mvn/raw/master/releases/"
)

//resolvers ++= Seq(
//  "anormcypher" at "http://repo.anormcypher.org/",
//  "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
//)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.2" ,
  "org.apache.spark" % "spark-streaming_2.10" % "1.5.2",
  "org.anormcypher" %% "anormcypher" % "0.4.2", // neo4j
  //"io.spray" %%  "spray-json" % "1.3.2", // json
  //"com.datastax.spark" %% "spark-cassandra-connector" % "1.5.0-RC1"
  //"org.json4s"  %% "json4s-native" % "3.3.0"
  "org.json4s"  %% "json4s-jackson" % "3.2.10" // json

  // connector spark orientdb
  //  "com.orientechnologies" % "orientdb-core" % "2.1.0",
  //  "com.orientechnologies" % "orientdb-client" % "2.1.0",
  //  "org.apache.spark" % "spark-core_2.11" % "1.4.0",
  //  "org.apache.spark" % "spark-graphx_2.11" % "1.4.0",
  //  "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0",
  //  "com.orientechnologies" % "orientdb-graphdb" % "2.1.0",
  //  "com.orientechnologies" % "orientdb-distributed" % "2.1.0"


)



