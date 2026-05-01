// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

ThisBuild / organization := "org.apache.texera"
ThisBuild / version      := "1.1.0-incubating"
ThisBuild / scalaVersion := "2.13.18"

// Per-module ASF licensing: each jar's META-INF/LICENSE describes only what is in that jar.
// Modules without vendored code get Apache 2.0 only; workflow-operator includes mbknor attribution.
// See project/AddMetaInfLicenseFiles.scala.
// Dist-producing modules additionally override Universal / mappings in their own
// build.sbt (not here) — see AddMetaInfLicenseFiles.distMappings.
lazy val asfLicensingSettings = AddMetaInfLicenseFiles.defaultSettings
lazy val asfLicensingSettingsWithVendored = AddMetaInfLicenseFiles.workflowOperatorSettings

val jacksonVersion = "2.18.6"

lazy val DAO = (project in file("common/dao")).settings(asfLicensingSettings)
lazy val Config = (project in file("common/config")).settings(asfLicensingSettings)
lazy val Auth = (project in file("common/auth"))
  .settings(asfLicensingSettings)
  .dependsOn(DAO, Config)
lazy val ConfigService = (project in file("config-service"))
  .dependsOn(Auth, Config)
  .settings(asfLicensingSettings)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
    )
  )
lazy val AccessControlService = (project in file("access-control-service"))
  .dependsOn(Auth, Config, DAO)
  .settings(asfLicensingSettings)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
    )
  )
  .configs(Test)
  .dependsOn(DAO % "test->test", Auth % "test->test")

//This Scala module defines a pyb"..." macro-based DSL for composing Python code templates as an immutable PythonTemplateBuilder.
//Used mainly for Python Native Operators
lazy val PyBuilder = (project in file("common/pybuilder"))
  .settings(asfLicensingSettings)
  .configs(Test)
  .dependsOn(DAO % "test->test") // test scope dependency

lazy val WorkflowCore = (project in file("common/workflow-core"))
  .settings(asfLicensingSettings)
  .dependsOn(DAO, Config, PyBuilder)
  .configs(Test)
  .dependsOn(DAO % "test->test") // test scope dependency
lazy val ComputingUnitManagingService = (project in file("computing-unit-managing-service"))
  .dependsOn(WorkflowCore, Auth, Config)
  .settings(asfLicensingSettings)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
    )
  )
lazy val FileService = (project in file("file-service"))
  .settings(asfLicensingSettings)
  .dependsOn(WorkflowCore, Auth, Config)
  .configs(Test)
  .dependsOn(DAO % "test->test") // test scope dependency
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "org.glassfish.jersey.core" % "jersey-common" % "3.0.12"
    )
  )

lazy val WorkflowOperator = (project in file("common/workflow-operator")).settings(asfLicensingSettingsWithVendored).dependsOn(WorkflowCore)
lazy val WorkflowCompilingService = (project in file("workflow-compiling-service"))
  .dependsOn(WorkflowOperator, Config)
  .settings(asfLicensingSettings)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "org.glassfish.jersey.core" % "jersey-common" % "3.0.12"
    )
  )

lazy val WorkflowExecutionService = (project in file("amber"))
  .dependsOn(WorkflowOperator, Auth, Config)
  .settings(asfLicensingSettings)
  .settings(
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "org.slf4j" % "slf4j-api" % "1.7.26",
      "org.eclipse.jetty" % "jetty-server" % "9.4.20.v20190813",
      "org.eclipse.jetty" % "jetty-servlet" % "9.4.20.v20190813",
      "org.eclipse.jetty" % "jetty-http" % "9.4.20.v20190813",
      // Netty dependency overrides to ensure compatibility with Arrow 14.0.1
      // Arrow requires Netty 4.1.96.Final to avoid NoSuchFieldError: chunkSize
      "io.netty" % "netty-all" % "4.1.96.Final",
      "io.netty" % "netty-buffer" % "4.1.96.Final",
      "io.netty" % "netty-codec" % "4.1.96.Final",
      "io.netty" % "netty-codec-http" % "4.1.96.Final",
      "io.netty" % "netty-codec-http2" % "4.1.96.Final",
      "io.netty" % "netty-common" % "4.1.96.Final",
      "io.netty" % "netty-handler" % "4.1.96.Final",
      "io.netty" % "netty-resolver" % "4.1.96.Final",
      "io.netty" % "netty-transport" % "4.1.96.Final",
      "io.netty" % "netty-transport-native-unix-common" % "4.1.96.Final"
    ),
    libraryDependencies ++= Seq(
      "com.squareup.okhttp3" % "okhttp" % "4.10.0" force () // Force usage of OkHttp 4.10.0
    )
  )
  .configs(Test)
  .dependsOn(DAO % "test->test", Auth % "test->test") // test scope dependency

// root project definition
lazy val TexeraProject = (project in file("."))
  .aggregate(
    DAO,
    Config,
    ConfigService,
    AccessControlService,
    Auth,
    WorkflowCore,
    ComputingUnitManagingService,
    FileService,
    WorkflowOperator,
    WorkflowCompilingService,
    WorkflowExecutionService
  )
  .settings(
    name := "texera",
    publishMavenStyle := true
  )
