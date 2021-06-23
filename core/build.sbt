ThisBuild / name := "texera"
ThisBuild / organization := "edu.uci.ics"
ThisBuild / version := "0.1-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.8"

lazy val core = (project in file("amber"))
  .settings()

lazy val util = (project in file("util"))
  .settings()
