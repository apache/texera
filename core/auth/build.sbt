/////////////////////////////////////////////////////////////////////////////
// Project Settings
/////////////////////////////////////////////////////////////////////////////

name := "auth"
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
// Dependencies
/////////////////////////////////////////////////////////////////////////////

// Core Dependencies
libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.3",                                  // config reader
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",            // for LazyLogging
  "org.bitbucket.b_c" % "jose4j" % "0.9.6",                             // for jwt parser
  "jakarta.ws.rs" % "jakarta.ws.rs-api" % "3.0.0"                       // for JwtAuthFilter
)