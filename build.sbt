name := "SparkSearchEngine"

version := "1.0"

scalaVersion := "2.12.15"

javacOptions ++= Seq("-source", "11", "-target", "11")

libraryDependencies ++= Seq(
  // Spark Core and SQL
  "org.mongodb" % "mongodb-driver-sync" % "4.11.1",
  "org.mongodb" % "bson" % "4.11.1",
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.4.0",
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",

  // Logging Dependencies
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "org.slf4j" % "slf4j-log4j12" % "1.7.36"
)

resolvers += "Maven Central" at "https://repo1.maven.org/maven2/"

scalacOptions ++= Seq(
  "-Ywarn-unused",
  "-Ywarn-unused-import"
)
