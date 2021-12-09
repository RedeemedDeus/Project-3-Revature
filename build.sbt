name := "kafka-client"

version := "0.1"

scalaVersion := "2.12.8"

lazy val root = (project in file("."))
  .settings(
    name := "kafka-client"
  )

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "2.3.1"