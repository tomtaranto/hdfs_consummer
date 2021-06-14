name := "hdfs_consummer"

version := "0.1"

scalaVersion := "2.12.13"



// https://mariuszprzydatek.com/2015/05/10/writing-files-to-hadoop-hdfs-using-scala/
libraryDependencies ++= Seq(  "org.apache.hadoop" % "hadoop-client" % "2.7.0")

libraryDependencies += "org.apache.kafka" %"kafka-clients" % "2.7.0"

