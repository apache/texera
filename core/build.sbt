lazy val DAO = project in file("dao")
lazy val WorkflowCore = (project in file("workflow-core"))
  .dependsOn(DAO)
  .configs(Test)
  .dependsOn(DAO % "test->test") // test scope dependency
lazy val WorkflowOperator = (project in file("workflow-operator"))
  .dependsOn(WorkflowCore)
  .settings(
    dependencyOverrides ++= Seq(
      "org.apache.commons" % "commons-compress" % "1.23.0", // because of the dependency introduced by iceberg
    )
  )
lazy val WorkflowCompilingService = (project in file("workflow-compiling-service"))
  .dependsOn(WorkflowOperator)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.16.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.16.1",
      "org.glassfish.jersey.core" % "jersey-common" % "3.0.12"
    )
  )

lazy val WorkflowExecutionService = (project in file("amber"))
  .dependsOn(WorkflowOperator)
  .settings(
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % "2.15.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.1",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.1",
      "org.slf4j" % "slf4j-api" % "1.7.26",
      "org.eclipse.jetty" % "jetty-server" % "9.4.20.v20190813",
      "org.eclipse.jetty" % "jetty-servlet" % "9.4.20.v20190813",
      "org.eclipse.jetty" % "jetty-http" % "9.4.20.v20190813",
    )
  )
  .configs(Test)
  .dependsOn(DAO % "test->test") // test scope dependency

// root project definition
lazy val CoreProject = (project in file("."))
  .aggregate(DAO, WorkflowCore, WorkflowOperator, WorkflowCompilingService, WorkflowExecutionService)
  .settings(
    name := "core",
    version := "0.1.0",
    organization := "edu.uci.ics",
    scalaVersion := "2.13.12",
    publishMavenStyle := true
  )
