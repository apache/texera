import scala.collection.Seq

name := "file-service"
organization := "edu.uci.ics"
version := "0.1.0"
scalaVersion := "2.13.12"

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
  "io.dropwizard" % "dropwizard-auth" % dropwizardVersion, // Dropwizard Authentication module
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.15.2",
  "jakarta.ws.rs" % "jakarta.ws.rs-api" % "3.1.0", // Ensure Jakarta JAX-RS API is available
  "org.bitbucket.b_c" % "jose4j" % "0.9.6",
  "org.playframework" %% "play-json" % "3.1.0-M1",
  "software.amazon.awssdk" % "s3" % "2.29.51",
  "software.amazon.awssdk" % "auth" % "2.29.51",
  "software.amazon.awssdk" % "regions" % "2.29.51",
)
