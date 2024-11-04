import scala.collection.Seq
/////////////////////////////////////////////////////////////////////////////
// Project Settings
/////////////////////////////////////////////////////////////////////////////

name := "workflow-operator"
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
// Test-related Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "org.scalamock" %% "scalamock" % "5.2.0" % Test,                  // ScalaMock
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,                 // ScalaTest
  "junit" % "junit" % "4.13.2" % Test,                              // JUnit
  "com.novocode" % "junit-interface" % "0.11" % Test                // SBT interface for JUnit
)


/////////////////////////////////////////////////////////////////////////////
// Jackson-related Dependencies
/////////////////////////////////////////////////////////////////////////////

val jacksonVersion = "2.15.1"
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,                  // Jackson Databind
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,               // Jackson Annotation
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,           // Scala Module
)

// Lucene related
val luceneVersion = "8.7.0"
libraryDependencies ++= Seq(
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-queries" % luceneVersion,
  "org.apache.lucene" % "lucene-memory" % luceneVersion
)

// kjetland
libraryDependencies ++= Seq(
  "javax.validation" % "validation-api" % "2.0.1.Final",
  "org.slf4j" % "slf4j-api" % "1.7.26",
  "io.github.classgraph" % "classgraph" % "4.8.157",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",
  "com.github.java-json-tools" % "json-schema-validator" % "2.2.14" % "test",
  "com.fasterxml.jackson.module" % "jackson-module-kotlin" % jacksonVersion % "test",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jacksonVersion % "test",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion % "test",
  "joda-time" % "joda-time" % "2.12.5" % "test",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonVersion % "test",
  "com.fasterxml.jackson.module" % "jackson-module-jsonSchema" % jacksonVersion,
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.13" % jacksonVersion,
  // https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-no-ctor-deser
  "com.fasterxml.jackson.module" % "jackson-module-no-ctor-deser" % jacksonVersion,
)

/////////////////////////////////////////////////////////////////////////////
// Additional Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "org.jgrapht" % "jgrapht-core" % "1.4.0",                      // JGraphT Core
  "org.eclipse.jgit" % "org.eclipse.jgit" % "5.13.0.202109080827-r", // JGit
  "com.thesamet.scalapb" %% "scalapb-json4s" % "0.12.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",       // csv parser
  "com.konghq" % "unirest-java" % "3.14.2",
  "commons-io" % "commons-io" % "2.15.1",
  "org.apache.commons" % "commons-compress" % "1.23.0",
  "org.tukaani" % "xz" % "1.9",
  "com.univocity" % "univocity-parsers" % "2.9.1",
  "edu.stanford.nlp" % "stanford-corenlp" % "4.5.4",
  "edu.stanford.nlp" % "stanford-corenlp" % "4.5.4" classifier "models",
  "io.github.redouane59.twitter" % "twittered" % "2.21",
)