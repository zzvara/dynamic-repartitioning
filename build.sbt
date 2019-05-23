import sbt.Credentials

name := "dynamic-repartitioning-core"

version := "0.2.0-SNAPSHOT"

organization := "hu.sztaki"

scalaVersion := "2.12.8"

resolvers ++= Seq(
  "Soft-props repository" at "http://dl.bintray.com/content/softprops/maven",
  "Lightshed Maven repository" at "http://dl.bintray.com/content/lightshed/maven",
  "Seasar" at "https://www.seasar.org/maven/maven2/"
)

updateOptions := updateOptions.value.withGigahorse(false)

publishConfiguration := publishConfiguration.value.withOverwrite(true)

resolvers += Resolver.mavenLocal

libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "com.typesafe.scala-logging" % "scala-logging_2.12" % "3.5.0"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.7"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
libraryDependencies += "org.jboss.interceptor" % "jboss-interceptor-api" % "1.1"
libraryDependencies += "it.unimi.dsi" % "dsiutils" % "2.3.6"
libraryDependencies += "com.google.guava" % "guava" % "27.1-jre"

//libraryDependencies += "com.github.fzakaria" % "space-saving" % "1.0.1-SNAPSHOT"
libraryDependencies += "hu.sztaki" % "freq-count_2.12" % "2.0"
