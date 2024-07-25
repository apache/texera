import scala.collection.Seq

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.12"

lazy val root = (project in file("."))
  .settings(
    name := "workflow-pod-brain"
  )

val dropwizardVersion = "4.0.7"
// https://mvnrepository.com/artifact/io.dropwizard/dropwizard-core
val dropwizardDependencies = Seq(
  "io.dropwizard" % "dropwizard-core" % dropwizardVersion,

)



libraryDependencies ++= dropwizardDependencies

// https://mvnrepository.com/artifact/io.kubernetes/client-java
libraryDependencies += "io.kubernetes" % "client-java" % "21.0.0"

// jooq
libraryDependencies += "org.jooq" % "jooq" % "3.14.16"

// config
libraryDependencies += "com.typesafe" % "config" % "1.4.1"

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"