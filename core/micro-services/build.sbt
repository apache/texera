lazy val WorkflowCore = project in file("workflow-core")

// root project definition
lazy val MicroServices = (project in file("."))
  .aggregate(WorkflowCore)
  .settings(
    name := "micro-services",
    version := "0.1.0",
    organization := "edu.uci.ics",
    scalaVersion := "2.13.12",
    publishMavenStyle := true
  )
