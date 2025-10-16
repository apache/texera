/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/////////////////////////////////////////////////////////////////////////////
// Project Settings
/////////////////////////////////////////////////////////////////////////////
name := "mcp-service"
organization := "org.apache"
version := "1.0.0"
scalaVersion := "2.13.12"

enablePlugins(JavaAppPackaging)

// Enable semanticdb for Scalafix
ThisBuild / semanticdbEnabled := true
ThisBuild / semanticdbVersion := scalafixSemanticdb.revision

// Manage dependency conflicts
ThisBuild / conflictManager := ConflictManager.latestRevision

// Restrict parallel test execution
Global / concurrentRestrictions += Tags.limit(Tags.Test, 1)

/////////////////////////////////////////////////////////////////////////////
// Compiler Options
/////////////////////////////////////////////////////////////////////////////

Compile / scalacOptions ++= Seq(
  "-Xelide-below", "WARNING",
  "-feature",
  "-deprecation",
  "-Ywarn-unused:imports",
)

/////////////////////////////////////////////////////////////////////////////
// Version Variables
/////////////////////////////////////////////////////////////////////////////
val dropwizardVersion = "4.0.7"
val mockitoVersion = "5.4.0"
val assertjVersion = "3.24.2"
val jacksonVersion = "2.17.0"
val reactorVersion = "3.6.0"

/////////////////////////////////////////////////////////////////////////////
// Test Dependencies
/////////////////////////////////////////////////////////////////////////////
libraryDependencies ++= Seq(
  "org.scalamock" %% "scalamock" % "5.2.0" % Test,
  "org.scalatest" %% "scalatest" % "3.2.17" % Test,
  "io.dropwizard" % "dropwizard-testing" % dropwizardVersion % Test,
  "org.mockito" % "mockito-core" % mockitoVersion % Test,
  "org.assertj" % "assertj-core" % assertjVersion % Test,
  // JUnit 5 (Jupiter) for Java tests
  "org.junit.jupiter" % "junit-jupiter-api" % "5.10.1" % Test,
  "org.junit.jupiter" % "junit-jupiter-engine" % "5.10.1" % Test,
  "org.junit.jupiter" % "junit-jupiter-params" % "5.10.1" % Test,
  "com.github.sbt" % "junit-interface" % "0.13.3" % Test
)

/////////////////////////////////////////////////////////////////////////////
// Core Dependencies
/////////////////////////////////////////////////////////////////////////////
libraryDependencies ++= Seq(
  // Dropwizard
  "io.dropwizard" % "dropwizard-core" % dropwizardVersion,
  "io.dropwizard" % "dropwizard-auth" % dropwizardVersion,

  // Jackson
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,

  // MCP SDK (Note: These are placeholder versions - update with actual versions when available)
  // For now, we'll implement MCP protocol directly
  "io.modelcontextprotocol.sdk" % "mcp" % "0.14.1",
  "io.modelcontextprotocol.sdk" % "mcp-json-jackson2" % "0.14.1",

  // Reactive Streams (for async support)
  "io.projectreactor" % "reactor-core" % reactorVersion,

  // Logging
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",
  "ch.qos.logback" % "logback-classic" % "1.4.14",

  // WebSocket support (for potential future MCP transport)
  "org.java-websocket" % "Java-WebSocket" % "1.5.4",

  // JSON Schema generation (reuse from WorkflowOperator)
  "com.kjetland" % "mbknor-jackson-jsonschema_2.13" % "1.0.39"
)