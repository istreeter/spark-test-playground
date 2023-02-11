import Dependencies._

ThisBuild / scalaVersion     := "2.13.10"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "Spark Playground",
    libraryDependencies ++= Seq(
      Dependencies.sparkCore,
      Dependencies.sparkSql,
      Dependencies.sparkHive,
      Dependencies.delta
    )
  )

fork := true

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.
