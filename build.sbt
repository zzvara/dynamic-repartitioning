import sbt.Credentials

name := "dynamic-repartitioning-core"

version := "0.2.0-SNAPSHOT"

organization := "hu.sztaki"

scalaVersion := "2.12.11"

resolvers ++= Seq("Maven Central" at "https://repo1.maven.org/maven2/")

updateOptions := updateOptions.value.withGigahorse(false)

publishConfiguration := publishConfiguration.value.withOverwrite(true)

libraryDependencies += "com.typesafe" % "config" % "1.4.0"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.9"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % Test
libraryDependencies += "it.unimi.dsi" % "dsiutils" % "2.6.3"
/**
  * @note This can't be upgraded due to dsiutils
  */
libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.0-alpha1"
libraryDependencies += "com.google.guava" % "guava" % "28.2-jre"
libraryDependencies += "hu.sztaki" %% "freq-count" % "2.0"
