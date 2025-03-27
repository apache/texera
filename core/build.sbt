lazy val DAO = project in file("dao")
lazy val WorkflowCore = (project in file("workflow-core"))
  .dependsOn(DAO)
  .configs(Test)
  .dependsOn(DAO % "test->test") // test scope dependency
lazy val Auth = (project in file("auth"))
  .dependsOn(DAO)
lazy val FileService = (project in file("file-service"))
  .dependsOn(WorkflowCore, Auth)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.16.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.16.1",
      "org.glassfish.jersey.core" % "jersey-common" % "3.0.12"
    )
  )

lazy val WorkflowOperator = (project in file("workflow-operator")).dependsOn(WorkflowCore)
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
  .dependsOn(WorkflowOperator, Auth)
  .settings(
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % "2.15.1",
      "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.1",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.1",
      "org.slf4j" % "slf4j-api" % "1.7.26",
      "org.eclipse.jetty" % "jetty-server" % "9.4.20.v20190813",
      "org.eclipse.jetty" % "jetty-servlet" % "9.4.20.v20190813",
      "org.eclipse.jetty" % "jetty-http" % "9.4.20.v20190813",
    ),
    libraryDependencies ++= Seq(
      "com.squareup.okhttp3" % "okhttp" % "4.10.0" force(), // Force usage of OkHttp 4.10.0
    ),
  )
  .configs(Test)
  .dependsOn(DAO % "test->test", Auth % "test->test") // test scope dependency

// root project definition
lazy val CoreProject = (project in file("."))
  .aggregate(DAO, Auth, WorkflowCore, FileService, WorkflowOperator, WorkflowCompilingService, WorkflowExecutionService)
  .settings(
    name := "core",
    version := "0.1.0",
    organization := "edu.uci.ics",
    scalaVersion := "2.13.12",
    publishMavenStyle := true
  )
