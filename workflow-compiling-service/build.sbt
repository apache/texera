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

/////////////////////////////////////////////////////////////////////////////
// Project Settings
/////////////////////////////////////////////////////////////////////////////

name := "workflow-compiling-service"


enablePlugins(JavaAppPackaging)

// Ship LICENSE-binary, NOTICE-binary, DISCLAIMER, and the licenses/
// directory at the top of the Universal dist zip.
// See project/AddMetaInfLicenseFiles.scala.
Universal / mappings := AddMetaInfLicenseFiles.distMappings(
  (Universal / mappings).value,
  (ThisBuild / baseDirectory).value,
  baseDirectory.value / "LICENSE-binary",
  baseDirectory.value / "NOTICE-binary"
)

// Enable semanticdb for Scalafix
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// Manage dependency conflicts by always using the latest revision
ThisBuild / conflictManager := ConflictManager.latestRevision

// Restrict parallel execution of tests to avoid conflicts. This caps how many
// test *suites* run concurrently; ParallelTestExecution still parallelizes the
// tests *within* a suite (e.g. OperatorBehaviorSpec) via ScalaTest's own pool.
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

// The fast-unit / integration test split; the selection logic itself is shared
// in project/TestFilters.scala.
Test / testOptions ++= TestFilters.integrationSplit(
  envVar = "WCS_TEST_FILTER",
  tag = "org.apache.texera.amber.translator.verify.tags.IntegrationTest"
)

// -P4 bounds ScalaTest's ParallelTestExecution pool, and only this module wants
// it: OperatorBehaviorSpec forks a Python subprocess per operator, and at
// core-count concurrency (e.g. 12) resource contention caused rare flakes. A
// fixed 4 stays deterministic across machines (incl. CI runners) while still
// running ~3x faster than serial, and it matches PythonWorkerPool's own default
// worker cap so the two bounds agree rather than multiply. Unconditional, so a
// local run reproduces the concurrency CI runs at instead of a faster one that
// flakes differently; WCS_TEST_FILTER selects which tests run, which is a
// separate question from how many run at once. The fast-unit job is unaffected
// either way, since OperatorBehaviorSpec is the only spec here that
// parallelizes and that job excludes it. It lives here rather than in the
// shared helper so that helper stays identical for every module. sbt
// concatenates the ScalaTest arguments of every testOptions entry, so this
// lands in the same argument list as the -n above.
Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-P4")

/////////////////////////////////////////////////////////////////////////////
// Compiler Options
/////////////////////////////////////////////////////////////////////////////

// Scala compiler options
Compile / scalacOptions ++= Seq(
  "-Xelide-below", "WARNING",       // Turn on optimizations with "WARNING" as the threshold
  "-feature",                       // Check feature warnings
  "-deprecation",                   // Check deprecation warnings
  "-Ywarn-unused:imports"           // Check for unused imports
)

/////////////////////////////////////////////////////////////////////////////
// Version Variables
/////////////////////////////////////////////////////////////////////////////

val dropwizardVersion = "4.0.7"
val mockitoVersion = "5.4.0"
val assertjVersion = "3.27.7"

/////////////////////////////////////////////////////////////////////////////
// Test-related Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "org.scalamock" %% "scalamock" % "5.2.0" % Test,                   // ScalaMock
  "org.scalatest" %% "scalatest" % "3.2.20" % Test,                  // ScalaTest
  "io.dropwizard" % "dropwizard-testing" % dropwizardVersion % Test, // Dropwizard Testing
  "org.mockito" % "mockito-core" % mockitoVersion % Test,            // Mockito for mocking
  "org.assertj" % "assertj-core" % assertjVersion % Test,            // AssertJ for assertions
  "com.novocode" % "junit-interface" % "0.11" % Test                // SBT interface for JUnit
)

/////////////////////////////////////////////////////////////////////////////
// Dependencies
/////////////////////////////////////////////////////////////////////////////

// Core Dependencies
libraryDependencies ++= Seq(
  "io.dropwizard" % "dropwizard-core" % dropwizardVersion,
  "io.dropwizard" % "dropwizard-auth" % dropwizardVersion, // Dropwizard Authentication module
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.8"
)
