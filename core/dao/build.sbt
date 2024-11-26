/////////////////////////////////////////////////////////////////////////////
// Project Settings
/////////////////////////////////////////////////////////////////////////////

name := "dao"
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
// ScalaPB Configuration
/////////////////////////////////////////////////////////////////////////////

// Exclude some proto files
PB.generate / excludeFilter := "scalapb.proto"

// Set the protoc version for ScalaPB
ThisBuild / PB.protocVersion := "3.19.4"

// ScalaPB code generation for .proto files
Compile / PB.targets := Seq(
  scalapb.gen(singleLineToProtoString = true) -> (Compile / sourceManaged).value
)

// Mark the ScalaPB-generated directory as a generated source root
Compile / managedSourceDirectories += (Compile / sourceManaged).value

// ScalaPB library dependencies
libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf",
  "com.thesamet.scalapb" %% "scalapb-json4s" % "0.12.0"  // For ScalaPB 0.11.x
)

// Enable protobuf compilation in Test
Test / PB.protoSources += PB.externalSourcePath.value


/////////////////////////////////////////////////////////////////////////////
// Test-related Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "org.scalamock" %% "scalamock" % "5.2.0" % Test,                  // ScalaMock
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,                 // ScalaTest
  "junit" % "junit" % "4.13.2" % Test,                              // JUnit
  "com.novocode" % "junit-interface" % "0.11" % Test,               // SBT interface for JUnit
  "ch.vorburger.mariaDB4j" % "mariaDB4j" % "2.4.0" % Test           // for mocking DB
)

/////////////////////////////////////////////////////////////////////////////
// Jooq-related Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "org.jooq" % "jooq" % "3.14.16",
  "org.jooq" % "jooq-codegen" % "3.12.4"
)

/////////////////////////////////////////////////////////////////////////////
// Additional Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "mysql" % "mysql-connector-java" % "8.0.33",          // MySQL connector
)