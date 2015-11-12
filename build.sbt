name := "akka-stream-java8"

version := "1.0"

scalaVersion := "2.11.7"


resolvers += "kender" at "http://dl.bintray.com/kender/maven"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-stream-experimental" % "1.0" withSources(),
  "org.scala-lang.modules"      %% "scala-java8-compat"  % "0.3.0" withSources(),
  "com.timcharper" %% "acked-streams" % "1.0-RC1" withSources() withJavadoc(),
  "me.enkode" %% "java8-converters" % "1.1.0" withSources()
)

fork in run := true
