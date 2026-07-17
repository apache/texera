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

// Test-filter switch driven by the WCS_TEST_FILTER env var so the
// workflow-compiling-service and workflow-compiling-service-integration CI jobs
// select disjoint subsets by ScalaTest tag. Mirrors amber's AMBER_TEST_FILTER.
//   skip-integration : exclude @IntegrationTest-tagged specs (fast unit job;
//                      these fork Python, which that job does not provision)
//   integration-only : include only @IntegrationTest-tagged specs (the job
//                      that installs Python deps)
// Unset (default) runs everything — the normal local behavior.
Test / testOptions ++= (sys.env.get("WCS_TEST_FILTER") match {
  case Some("skip-integration") =>
    Seq(
      Tests.Argument(
        TestFrameworks.ScalaTest,
        "-l",
        "org.apache.texera.amber.translator.verify.tags.IntegrationTest"
      )
    )
  case Some("integration-only") =>
    // -n <tag> : run only @IntegrationTest specs.
    // -P4      : bound ScalaTest's ParallelTestExecution pool to 4 threads.
    //            OperatorBehaviorSpec forks a Python subprocess per operator;
    //            at core-count concurrency (e.g. 12) that caused rare flakes
    //            from resource contention. A fixed 4 keeps it deterministic
    //            across machines (incl. CI runners) while still ~3x serial.
    Seq(
      Tests.Argument(
        TestFrameworks.ScalaTest,
        "-n",
        "org.apache.texera.amber.translator.verify.tags.IntegrationTest",
        "-P4"
      )
    )
  case _ => Nil
})

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
val assertjVersion = "3.24.2"

/////////////////////////////////////////////////////////////////////////////
// Test-related Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "org.scalamock" %% "scalamock" % "5.2.0" % Test,                   // ScalaMock
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,                  // ScalaTest
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
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.18.6"
)
