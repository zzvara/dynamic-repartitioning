import sbt.Credentials

name := "dynamic-repartitioning-core"

version := "0.1.72-SNAPSHOT"

organization := "hu.sztaki"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "JBoss" at "https://repository.jboss.org/",
  "Alpha Artifactory" at "http://alpha:7777/artifactory/sbt-dev-local/"
)

resolvers += Resolver.mavenLocal

libraryDependencies += "com.typesafe" % "config" % "1.3.1"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "org.apache.commons" % "commons-lang3" % "3.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"
libraryDependencies += "org.jboss.interceptor" % "jboss-interceptor-api" % "1.1"
libraryDependencies += "it.unimi.dsi" % "dsiutils" % "2.3.6"
//libraryDependencies += "com.github.fzakaria" % "space-saving" % "1.0.1-SNAPSHOT"
libraryDependencies += "hu.sztaki" % "freq-count_2.11" % "1.0"
libraryDependencies += "com.google.guava" % "guava" % "14.0.1"

dependencyOverrides += "com.google.guava" % "guava" % "14.0.1"

publishTo := Some("Artifactory Realm" at "http://alpha:7777/artifactory/sbt-dev-local/")
credentials += Credentials("Artifactory Realm", "alpha", "local", "czht5n947x9n2y")