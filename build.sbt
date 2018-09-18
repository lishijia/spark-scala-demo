name := "spark-scala-demo"

version := "0.1"

scalaVersion := "2.11.8"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.0"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.12"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.12"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.1.12"
