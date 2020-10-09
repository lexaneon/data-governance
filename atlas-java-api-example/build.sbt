name := "atlas-java-api-example"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.atlas" % "atlas-client-v2" % "2.0.0"
// https://mvnrepository.com/artifact/org.apache.atlas/atlas-client-v2
//libraryDependencies += "org.apache.atlas" % "atlas-client-v2" % "2.1.0"

libraryDependencies += "org.apache.atlas" % "atlas-common" % "2.0.0"
// https://mvnrepository.com/artifact/org.apache.atlas/atlas-common
//libraryDependencies += "org.apache.atlas" % "atlas-common" % "2.1.0"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4" % "provided"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.2.0" % "provided"