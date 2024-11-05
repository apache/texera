lazy val DAO = project in file("dao")
lazy val WorkflowCore = (project in file("workflow-core")).dependsOn(DAO)
lazy val WorkflowOperator = (project in file("workflow-operator")).dependsOn(WorkflowCore)
lazy val WorkflowCompilingService = (project in file("workflow-compiling-service")).dependsOn(WorkflowOperator)
// root project definition
lazy val MicroServices = (project in file("."))
  .aggregate(DAO, WorkflowCore, WorkflowOperator, WorkflowCompilingService)
  .settings(
    name := "micro-services",
    version := "0.1.0",
    organization := "edu.uci.ics",
    scalaVersion := "2.13.12",
    publishMavenStyle := true
  )
