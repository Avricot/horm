organization := "com.avricot"

name := "horm"

version := "0.1"

scalaVersion := "2.9.1"

//resolvers ++= Seq(
//)


libraryDependencies ++= Seq(
    "junit" % "junit" % "4.10" % "test",
    "org.apache.hbase" % "hbase" % "0.94.0" withSources(),
    "org.apache.hadoop" % "hadoop-common" % "0.23.1" withSources() ,
    "org.apache.hadoop" % "hadoop-auth" % "0.23.1" withSources() ,
    "joda-time" % "joda-time" % "2.1" withSources() ,
    "org.slf4j" % "slf4j-api" % "1.6.6" withSources() )


