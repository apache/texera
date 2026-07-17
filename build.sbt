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
ThisBuild / version      := "1.3.0-incubating-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.18"

// Pull JDK 17+ JVM flags from .jvmopts so every JVM the build launches sees the same list.
import com.typesafe.sbt.packager.universal.UniversalPlugin.autoImport.Universal
ThisBuild / Test / javaOptions ++=
  JdkOptions.jvmFlags((ThisBuild / baseDirectory).value)

// Fail Java compilation on deprecation warnings so PRs can't reintroduce
// deprecated-API patterns (e.g. scala.collection.JavaConverters in Java
// callers — the modern Java entry point is scala.jdk.javaapi.CollectionConverters).
// -Xlint:deprecation surfaces the per-call-site location, -Werror turns it fatal.
ThisBuild / Compile / javacOptions ++= Seq("-Xlint:deprecation", "-Werror")
// Emit one JUnit-XML file per spec under each module's target/test-reports/.
// Codecov Test Analytics ingests these via `report_type: test_results` to
// surface failing-test stack traces in PR comments and flag tests that have
// gone flaky on main. ScalaTest's `-u` argument is additive — module-level
// testOptions (e.g. amber/build.sbt's filter args) continue to apply.
ThisBuild / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-u", "target/test-reports")

// sbt-jacoco emits only HTML by default; add XML so Codecov can consume
// per-module jacoco.xml at target/scala-2.13/jacoco/report/jacoco.xml.
// JacocoPlugin defines a project-scoped default that overrides ThisBuild,
// so this Seq is folded into commonModuleSettings (applied to every module).
import com.github.sbt.jacoco.report.{JacocoReportFormats, JacocoReportSettings}
lazy val coverageReportSettings = Seq(
  jacocoReportSettings := JacocoReportSettings()
    .withTitle("Apache Texera Coverage")
    .withFormats(JacocoReportFormats.ScalaHTML, JacocoReportFormats.XML)
)

lazy val universalJvmFlagsSettings = Seq(
  Universal / javaOptions ++=
    JdkOptions.jvmFlags((ThisBuild / baseDirectory).value).map("-J" + _)
)

// Per-module ASF licensing: each jar's META-INF/LICENSE describes only what is in that jar.
// Modules without vendored code get Apache 2.0 only; workflow-operator includes mbknor attribution.
// See project/AddMetaInfLicenseFiles.scala.
// Dist-producing modules additionally override Universal / mappings in their own
// build.sbt (not here) — see AddMetaInfLicenseFiles.distMappings.
lazy val asfLicensingSettings = AddMetaInfLicenseFiles.defaultSettings
lazy val asfLicensingSettingsWithVendored = AddMetaInfLicenseFiles.workflowOperatorSettings

val bouncyCastleVersion = "1.84"

lazy val bouncyCastleOverrides = Seq(
  "org.bouncycastle" % "bcpkix-jdk18on" % bouncyCastleVersion,
  "org.bouncycastle" % "bcprov-jdk18on" % bouncyCastleVersion,
  "org.bouncycastle" % "bcutil-jdk18on" % bouncyCastleVersion
)

lazy val commonDependencyOverrides = Seq(
  dependencyOverrides ++= bouncyCastleOverrides
)

// Aggregate of the settings every module shares. These are independent
// concerns — ASF licensing, jacoco XML coverage, and universal JVM flags —
// grouped only so each module can apply them with a single .settings(...) call.
lazy val commonModuleSettings =
  asfLicensingSettings ++ coverageReportSettings ++ universalJvmFlagsSettings ++ commonDependencyOverrides
lazy val commonModuleSettingsWithVendored =
  asfLicensingSettingsWithVendored ++ coverageReportSettings ++ universalJvmFlagsSettings ++ commonDependencyOverrides

val jacksonVersion = "2.18.8"

// Netty must be pinned as a single coordinated family: Apache Arrow's
// arrow-memory-netty accesses Netty allocator internals, so a split across the
// 4.1/4.2 line breaks it (NoClassDefFoundError: ThreadAwareExecutor). Arrow
// 19.0.0 targets the Netty 4.2 line, so the whole family is held at 4.2.x here
// and in common/workflow-core/build.sbt.
val nettyVersion = "4.2.15.Final"

// The full Netty family pinned to nettyVersion. Applied to every module whose
// dist bundles Arrow Flight (amber + the Arrow-carrying platform services), so
// the 4.1/4.2 line can never split — arrow-memory-netty reaches into Netty
// allocator internals and a split breaks it. Kept in sync with the same list
// in common/workflow-core/build.sbt (which can't see this val).
val nettyDependencyOverrides = Seq(
  "io.netty" % "netty-all" % nettyVersion,
  "io.netty" % "netty-buffer" % nettyVersion,
  "io.netty" % "netty-codec" % nettyVersion,
  "io.netty" % "netty-codec-http" % nettyVersion,
  "io.netty" % "netty-codec-http2" % nettyVersion,
  "io.netty" % "netty-codec-socks" % nettyVersion,
  "io.netty" % "netty-common" % nettyVersion,
  "io.netty" % "netty-handler" % nettyVersion,
  "io.netty" % "netty-handler-proxy" % nettyVersion,
  "io.netty" % "netty-resolver" % nettyVersion,
  "io.netty" % "netty-transport" % nettyVersion,
  "io.netty" % "netty-transport-classes-epoll" % nettyVersion,
  "io.netty" % "netty-transport-native-epoll" % nettyVersion,
  "io.netty" % "netty-transport-native-unix-common" % nettyVersion
)

// Hadoop/ZooKeeper (declared in common/workflow-core and amber) pull in the
// EOL log4j 1.2.17, which has open CVEs and no fixed 1.x release. Keep it out
// of every module; the log4j 2.x bridges declared in common/workflow-core
// keep the org.apache.log4j API available at runtime.
ThisBuild / excludeDependencies += ExclusionRule("log4j", "log4j")

lazy val DAO = (project in file("common/dao")).settings(commonModuleSettings)
lazy val Config = (project in file("common/config")).settings(commonModuleSettings)
lazy val Resource = (project in file("common/resource")).settings(commonModuleSettings)
lazy val Auth = (project in file("common/auth"))
  .settings(commonModuleSettings)
  .configs(Test)
  .dependsOn(DAO, Config)
  .dependsOn(DAO % "test->test") // reuse MockTexeraDB embedded Postgres in tests
lazy val ConfigService = (project in file("config-service"))
  .dependsOn(Auth, Config, Resource)
  .settings(commonModuleSettings)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
    )
  )
lazy val AccessControlService = (project in file("access-control-service"))
  .dependsOn(Auth, Config, DAO, Resource)
  .settings(commonModuleSettings)
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
  .settings(commonModuleSettings)
  .configs(Test)
  .dependsOn(DAO % "test->test") // test scope dependency

lazy val WorkflowCore = (project in file("common/workflow-core"))
  .settings(commonModuleSettings)
  .dependsOn(DAO, Config, PyBuilder)
  .configs(Test)
  .dependsOn(DAO % "test->test") // test scope dependency
lazy val ComputingUnitManagingService = (project in file("computing-unit-managing-service"))
  .dependsOn(WorkflowCore, Auth, Config, Resource)
  .configs(Test)
  .dependsOn(DAO % "test->test") // reuse MockTexeraDB embedded Postgres in tests
  .settings(commonModuleSettings)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      // Arrow 19 (via WorkflowCore) evicts jackson-databind up to 2.21.0, past
      // the 2.18 range jackson-module-scala allows; pin it back to jacksonVersion
      // so the Scala module can initialize (else the service aborts at startup
      // with "Scala module 2.18.8 requires Jackson Databind version >= 2.18.0
      // and < 2.19.0 - Found jackson-databind version 2.21.0").
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion
    ) ++ nettyDependencyOverrides,
    // Fork the test JVM so the sharing feature flag can be enabled: ComputingUnitConfig
    // reads computing-unit.conf's sharing.enabled as a load-time val (default false,
    // overridable only via the COMPUTING_UNIT_SHARING_ENABLED env var), and the
    // access-resource tests need it on to reach the share/revoke code paths. Also run
    // from the repo root so MockTexeraDB can resolve sql/texera_ddl.sql by relative path.
    Test / fork := true,
    Test / envVars += "COMPUTING_UNIT_SHARING_ENABLED" -> "true",
    Test / forkOptions := (Test / forkOptions).value
      .withWorkingDirectory((ThisBuild / baseDirectory).value)
  )
lazy val FileService = (project in file("file-service"))
  .settings(commonModuleSettings)
  .dependsOn(WorkflowCore, Auth, Config, Resource)
  .configs(Test)
  .dependsOn(DAO % "test->test") // test scope dependency
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "org.glassfish.jersey.core" % "jersey-common" % "3.0.12"
    ) ++ nettyDependencyOverrides,
    // Each testcontainers-based suite starts its own LakeFS/MinIO/Postgres stack
    // and mutates JVM-wide singletons (StorageConfig endpoints, LakeFS client),
    // so every suite gets its own forked JVM; sbt runs forked groups one at a
    // time by default (Tags.ForkedTestGroup limit), keeping the stacks serial.
    Test / fork := true,
    Test / forkOptions := (Test / forkOptions).value
      .withWorkingDirectory((ThisBuild / baseDirectory).value),
    Test / testGrouping := (Test / definedTests).value.map { suite =>
      Tests.Group(suite.name, Seq(suite), Tests.SubProcess((Test / forkOptions).value))
    }
  )

lazy val WorkflowOperator = (project in file("common/workflow-operator")).settings(commonModuleSettingsWithVendored).dependsOn(WorkflowCore)
lazy val WorkflowCompilingService = (project in file("workflow-compiling-service"))
  .dependsOn(WorkflowOperator, Auth, Config, Resource)
  .settings(commonModuleSettings)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "org.glassfish.jersey.core" % "jersey-common" % "3.0.12"
    ) ++ nettyDependencyOverrides
  )

lazy val WorkflowExecutionService = (project in file("amber"))
  .dependsOn(WorkflowOperator, Auth, Config)
  .settings(commonModuleSettings)
  .settings(
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
      "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
      "org.slf4j" % "slf4j-api" % "1.7.26",
      "org.eclipse.jetty" % "jetty-server" % "9.4.20.v20190813",
      "org.eclipse.jetty" % "jetty-servlet" % "9.4.20.v20190813",
      "org.eclipse.jetty" % "jetty-http" % "9.4.20.v20190813"
    ) ++ nettyDependencyOverrides,
    libraryDependencies ++= Seq(
      "com.squareup.okhttp3" % "okhttp" % "4.10.0" force () // Force usage of OkHttp 4.10.0
    )
  )
  .configs(Test)
  .dependsOn(DAO % "test->test", Auth % "test->test") // test scope dependency
lazy val NotebookMigrationService = (project in file("notebook-migration-service"))
  .dependsOn(Auth, Config, DAO, Resource)
  .settings(commonModuleSettings)
  .settings(
    dependencyOverrides ++= Seq(
      // override it as io.dropwizard 4 require 2.16.1 or higher
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion
    )
  )
  .dependsOn(DAO % "test->test") // test scope dependency

// root project definition
lazy val TexeraProject = (project in file("."))
  .aggregate(
    // common libraries
    Auth,
    Config,
    Resource,
    DAO,
    PyBuilder,
    WorkflowCore,
    WorkflowOperator,
    // services
    AccessControlService,
    ComputingUnitManagingService,
    ConfigService,
    FileService,
    WorkflowCompilingService,
    WorkflowExecutionService,
    NotebookMigrationService
  )
  .settings(
    name := "texera",
    publishMavenStyle := true
  )
