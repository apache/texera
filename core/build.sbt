ThisBuild / organization := "com.example"
ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.13"

lazy val core = (project in file("amber"))
  .settings(
    // other settings
  )

lazy val util = (project in file("util"))
  .settings(
    // other settings
    // https://mvnrepository.com/artifact/org.jooq/jooq
    libraryDependencies += "org.jooq" % "jooq" % "3.14.4",
    // https://mvnrepository.com/artifact/org.jooq/jooq-codegen
    libraryDependencies += "org.jooq" % "jooq-codegen" % "3.12.4"
  )
