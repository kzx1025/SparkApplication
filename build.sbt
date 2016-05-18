name := "SparkApplication"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.1"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.1"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"

libraryDependencies += "org.apache.spark" % "spark-mllib_2.10"  % "1.6.1"

//libraryDependencies += "org.apache.spark" %% "spark-mllib_2.10" % "1.6.1"