name := "util"
organization := "edu.uci.ics"
version := "0.1-SNAPSHOT"

scalaVersion := "2.12.8"

lazy val util = project
  .in(file("."))
  .settings(
    // https://mvnrepository.com/artifact/mysql/mysql-connector-java
    libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.23",
    // https://mvnrepository.com/artifact/com.typesafe/config
    libraryDependencies += "com.typesafe" % "config" % "1.4.1",
    // https://mvnrepository.com/artifact/org.jooq/jooq
    libraryDependencies += "org.jooq" % "jooq" % "3.14.4",
    // https://mvnrepository.com/artifact/org.jooq/jooq-codegen
    libraryDependencies += "org.jooq" % "jooq-codegen" % "3.12.4"
  )
