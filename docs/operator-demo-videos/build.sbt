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

import scala.collection.Seq

/////////////////////////////////////////////////////////////////////////////
// Project Settings
/////////////////////////////////////////////////////////////////////////////

name := "operator-demo-videos"

// Enable semanticdb for Scalafix
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// Restrict parallel execution of tests to avoid conflicts
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

/////////////////////////////////////////////////////////////////////////////
// Compiler Options
/////////////////////////////////////////////////////////////////////////////

Compile / scalacOptions ++= Seq(
  "-Xelide-below", "WARNING",       // Turn on optimizations with "WARNING" as the threshold
  "-feature",                       // Check feature warnings
  "-deprecation",                   // Check deprecation warnings
  "-Ywarn-unused:imports"           // Check for unused imports
)

/////////////////////////////////////////////////////////////////////////////
// Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  // Drives a real browser to record the operator demos.
  "com.microsoft.playwright" % "playwright" % "1.57.0",
  "org.scalatest" %% "scalatest" % "3.2.20" % Test
)

// Playwright's transitive jackson (2.21.x) breaks jackson-module-scala 2.18.x from the
// operator-metadata modules; keep jackson on the repo-wide version (root build.sbt).
val jacksonVersion = "2.18.8"

dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion
)
