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

name := "config"


enablePlugins(JavaAppPackaging)

// Enable semanticdb for Scalafix
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// Manage dependency conflicts by always using the latest revision
ThisBuild / conflictManager := ConflictManager.latestRevision

// Restrict parallel execution of tests to avoid conflicts
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

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
// Dependencies
/////////////////////////////////////////////////////////////////////////////

// OpenTelemetry version is pinned here as the single source of truth; all
// services pick it up transitively via dependsOn(Config). Bump deliberately.
val openTelemetryVersion = "1.50.0"

// Core Dependencies
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.6",                                  // For configuration management
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",            // for LazyLogging in OtelInit
  // OpenTelemetry SDK bootstrap (Apache-2.0). We deliberately do NOT use
  // sdk-extension-autoconfigure: the security model requires that endpoint
  // + resource-attribute filtering run before any exporter is configured.
  "io.opentelemetry" % "opentelemetry-api" % openTelemetryVersion,
  "io.opentelemetry" % "opentelemetry-sdk" % openTelemetryVersion,
  "io.opentelemetry" % "opentelemetry-exporter-otlp" % openTelemetryVersion,
  // Logback Classic — needed at compile time to write the OTel log
  // appender. Marked `provided` because every service already brings
  // Logback in transitively (via Dropwizard / SLF4J), so we don't
  // bundle a second copy.
  "ch.qos.logback" % "logback-classic" % "1.2.13" % "provided",
  // Test-only: in-memory exporter for OtelInitSpec; avoids hitting a real
  // collector during unit tests.
  "io.opentelemetry" % "opentelemetry-sdk-testing" % openTelemetryVersion % Test,
  "ch.qos.logback" % "logback-classic" % "1.2.13" % Test,
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)