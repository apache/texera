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

name := "workflow-operator"


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

val jacksonVersion = "2.18.6"
libraryDependencies ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,                  // Jackson Databind
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,               // Jackson Annotation
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,           // Scala Module
)

// Lucene related, used by the keyword-search operators
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
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  // https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-no-ctor-deser
  "com.fasterxml.jackson.module" % "jackson-module-no-ctor-deser" % jacksonVersion,
)

/////////////////////////////////////////////////////////////////////////////
// Additional Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "scalapb-json4s" % "0.12.0",
  "com.github.tototoshi" %% "scala-csv" % "1.3.10",       // csv parser
  "com.konghq" % "unirest-java" % "3.14.2",
  "commons-io" % "commons-io" % "2.15.1",
  "org.apache.commons" % "commons-compress" % "1.27.1",
  "org.tukaani" % "xz" % "1.9",
  "com.univocity" % "univocity-parsers" % "2.9.1",
  "org.apache.lucene" % "lucene-analyzers-common" % "8.11.4"
)

// SmartFileSource: Parquet + Excel support.
//
// Hadoop drags in a LOT of stuff Texera doesn't use, and several of those
// transitive deps conflict head-on with Texera's existing Dropwizard + Jersey-3
// stack. We exclude all of the known troublemakers here. If you're tempted to
// remove one of these, run TexeraWebApplication and watch it die at startup.
//
// Conflicts being avoided:
//   - slf4j-reload4j / reload4j: conflicts with the project's logback setup
//   - jsp-api 2.1: ships an ancient `javax.el.ExpressionFactory` (no
//     `newInstance()`) that shadows the real `javax.el-3.0.x` Dropwizard's
//     Hibernate Validator needs (NoSuchMethodError otherwise)
//   - com.sun.jersey.* (Jersey 1.x): collides with the project's Jersey 3 via
//     HK2 — JSONRootElementProvider gets instantiated and explodes on init
//   - tomcat / jasper: only used by Hadoop's embedded web UIs
//   - servlet-api 2.5: ancient javax servlet that conflicts with Jakarta
libraryDependencies ++= Seq(
  "org.apache.parquet" % "parquet-hadoop" % "1.13.1",
  "org.apache.hadoop" % "hadoop-common" % "3.3.6"
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("javax.servlet.jsp", "jsp-api")
    exclude("javax.servlet", "servlet-api")
    exclude("org.mortbay.jetty", "jetty")
    exclude("org.mortbay.jetty", "jetty-util")
    exclude("org.mortbay.jetty", "jsp-api-2.1")
    exclude("tomcat", "jasper-compiler")
    exclude("tomcat", "jasper-runtime")
    exclude("com.sun.jersey", "jersey-core")
    exclude("com.sun.jersey", "jersey-server")
    exclude("com.sun.jersey", "jersey-json")
    exclude("com.sun.jersey", "jersey-servlet")
    exclude("com.sun.jersey", "jersey-client")
    excludeAll(ExclusionRule(organization = "com.sun.jersey")),
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.3.6"
    exclude("org.slf4j", "slf4j-reload4j")
    exclude("ch.qos.reload4j", "reload4j")
    exclude("javax.servlet.jsp", "jsp-api")
    exclude("javax.servlet", "servlet-api")
    excludeAll(ExclusionRule(organization = "com.sun.jersey")),
  "org.apache.poi" % "poi-ooxml" % "5.2.5"
)
// Global Hadoop transitive-dep blackhole is declared at the top-level
// build.sbt as `ThisBuild / excludeDependencies` so it applies to every
// downstream project (especially amber) that pulls Hadoop through us.

libraryDependencies += "io.github.classgraph" % "classgraph" % "4.8.184" % Test
