organization := "com.avricot"

name := "horm"

version := "0.2-SNAPSHOT"

scalaVersion := "2.9.1"

publishMavenStyle := true

publishTo <<= version { (v: String) =>
  val nexus = "https://oss.sonatype.org/"
  if (v.trim.endsWith("SNAPSHOT")) 
    Some("snapshots" at nexus + "content/repositories/snapshots") 
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

licenses := Seq("Apache Licence version 2.0" -> url("http://www.apache.org/licenses/"))

homepage := Some(url("https://github.com/Avricot/horm"))

pomExtra := (
  <scm>
    <url>git@github.com:Avricot/horm.git</url>
    <connection>scm:git:git@github.com:Avricot/horm.git</connection>
  </scm>
  <developers>
    <developer>
      <id>qambard</id>
      <name>Quentin Ambard</name>
      <url>http://avricot.com</url>
    </developer>
  </developers>)
  
//resolvers ++= Seq(
//)


libraryDependencies ++= Seq(
    "junit" % "junit" % "4.10" % "test",
    "org.apache.hbase" % "hbase" % "0.94.0",
    "org.apache.hadoop" % "hadoop-common" % "0.23.1" ,
    "org.apache.hadoop" % "hadoop-auth" % "0.23.1" ,
    "joda-time" % "joda-time" % "2.1" ,
    "com.google.guava" % "guava" % "r09" ,
    "org.slf4j" % "slf4j-api" % "1.6.6" )


