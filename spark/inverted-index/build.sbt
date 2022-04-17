ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "inverted-index"
  )

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.1"
