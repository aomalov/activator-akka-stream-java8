name := "akka-stream-java8"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0",
  "com.timcharper" %% "acked-streams" % "1.0-RC1"
)

fork in run := true
