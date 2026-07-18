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
// common/test — TEST-ONLY shared utilities
//
// Home for code that exists solely to support other modules' test suites:
// today the shared ScalaTest tags (@IntegrationTest / IntegrationTestTag);
// shared fixtures or matchers belong here too if they ever outgrow a single
// module. The sources live in the Compile scope only so that downstream
// modules can reach them from their Test configuration via
// `.dependsOn(TestUtil % "test")` in the root build.sbt.
//
// Never add this module as a Compile-scope dependency and never bundle it
// into a service dist — production code must not reference it.
/////////////////////////////////////////////////////////////////////////////

name := "test"

// Enable semanticdb for Scalafix
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// Manage dependency conflicts by always using the latest revision
ThisBuild / conflictManager := ConflictManager.latestRevision

/////////////////////////////////////////////////////////////////////////////
// Compiler Options
/////////////////////////////////////////////////////////////////////////////

// Scala compiler options (same set as the other common modules; scalafix's
// RemoveUnused rule requires -Ywarn-unused)
Compile / scalacOptions ++= Seq(
  "-Xelide-below",
  "WARNING", // Turn on optimizations with "WARNING" as the threshold
  "-feature", // Check feature warnings
  "-deprecation", // Check deprecation warnings
  "-Ywarn-unused:imports" // Check for unused imports
)

/////////////////////////////////////////////////////////////////////////////
// Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  // Compile scope: the shared tag annotations are main sources so that
  // downstream modules can reach them from their Test scope.
  "org.scalatest" %% "scalatest-core" % "3.2.20" // @TagAnnotation
)
