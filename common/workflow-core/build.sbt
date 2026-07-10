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

name := "workflow-core"


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

val testcontainersVersion = "0.44.1"

libraryDependencies ++= Seq(
  "org.scalamock" %% "scalamock" % "5.2.0" % Test,                  // ScalaMock
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,                 // ScalaTest
  "junit" % "junit" % "4.13.2" % Test,                              // JUnit
  "com.novocode" % "junit-interface" % "0.11" % Test,               // SBT interface for JUnit
  "com.dimafeng" %% "testcontainers-scala-scalatest" % testcontainersVersion % Test,   // Testcontainers ScalaTest integration
  "com.dimafeng" %% "testcontainers-scala-minio" % testcontainersVersion % Test,       // MinIO Testcontainer Scala integration
  "com.dimafeng" %% "testcontainers-scala-postgresql" % testcontainersVersion % Test   // Postgres Testcontainer (LakeFS metadata store)
)


/////////////////////////////////////////////////////////////////////////////
// Jackson-related Dependencies
/////////////////////////////////////////////////////////////////////////////

val jacksonVersion = "2.18.8"
libraryDependencies ++= Seq(
  "javax.validation" % "validation-api" % "2.0.1.Final",
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,        // Jackson Databind
  "com.fasterxml.jackson.module" % "jackson-module-kotlin" % jacksonVersion % Test,   // Jackson Kotlin Module
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jacksonVersion % Test, // Jackson JDK8 Datatypes
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion % Test, // Jackson JSR310
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonVersion % Test,   // Jackson Joda
  "com.fasterxml.jackson.module" % "jackson-module-jsonSchema" % jacksonVersion,      // JSON Schema Module
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,           // Scala Module
  "com.fasterxml.jackson.module" % "jackson-module-no-ctor-deser" % jacksonVersion     // No Constructor Deserializer
)

/////////////////////////////////////////////////////////////////////////////
// Arrow related
val arrowVersion = "19.0.0"
val nettyVersion = "4.2.15.Final"
val arrowDependencies = Seq(
  // flight-core bundles the gRPC transport since Arrow 16; the standalone
  // flight-grpc artifact was discontinued after 15.0.2.
  // https://mvnrepository.com/artifact/org.apache.arrow/flight-core
  "org.apache.arrow" % "flight-core" % arrowVersion
)

libraryDependencies ++= arrowDependencies

// Netty dependency overrides to ensure compatibility with Arrow 19.0.0, which
// targets the Netty 4.2 line. The whole family must be pinned to one version:
// arrow-memory-netty reaches into Netty allocator internals, so a 4.1/4.2 split
// breaks it (NoClassDefFoundError: ThreadAwareExecutor). Kept in sync with the
// same list in the root build.sbt (amber module).
dependencyOverrides ++= Seq(
  "io.netty" % "netty-all" % nettyVersion,
  "io.netty" % "netty-buffer" % nettyVersion,
  "io.netty" % "netty-codec" % nettyVersion,
  "io.netty" % "netty-codec-http" % nettyVersion,
  "io.netty" % "netty-codec-http2" % nettyVersion,
  "io.netty" % "netty-codec-socks" % nettyVersion,
  "io.netty" % "netty-common" % nettyVersion,
  "io.netty" % "netty-handler" % nettyVersion,
  "io.netty" % "netty-handler-proxy" % nettyVersion,
  "io.netty" % "netty-resolver" % nettyVersion,
  "io.netty" % "netty-transport" % nettyVersion,
  "io.netty" % "netty-transport-classes-epoll" % nettyVersion,
  "io.netty" % "netty-transport-native-epoll" % nettyVersion,
  "io.netty" % "netty-transport-native-unix-common" % nettyVersion,
  // Arrow 19's transitive deps pull jackson-databind past the 2.18 line that
  // jackson-module-scala is pinned to; force the Jackson core family back to
  // jacksonVersion so the Scala module can initialize (else Test aborts with
  // "Scala module 2.18.x requires Jackson Databind version >= 2.18.0 and
  // < 2.19.0").
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion
)

/////////////////////////////////////////////////////////////////////////////
// Iceberg-related Dependencies
/////////////////////////////////////////////////////////////////////////////
val excludeJersey = ExclusionRule(organization = "com.sun.jersey")
// Hadoop 3.3.2+ ships jersey-json via the com.github.pjfanning fork; its Jersey 1.x
// providers break Jersey 2 auto-discovery at startup, so exclude it as well.
val excludeJerseyJsonFork = ExclusionRule(organization = "com.github.pjfanning", name = "jersey-json")
// Hadoop 3.5.0 moved its web stack from Jersey 1.x (com.sun.jersey) to Jersey 2.x
// (org.glassfish.jersey.{core,containers,inject}) plus the glassfish JAXB runtime,
// istack-commons, and the Jakarta JSP API. ExclusionRule matches the organization string
// exactly, so a bare "org.glassfish.jersey" rule matches none of the real artifacts.
// Texera uses hadoop only as a filesystem client, so none of this servlet/JAX-RS/JAXB web
// stack is needed. Excluding it here keeps those jars out of every downstream service dist
// and, for services still on a Jersey 2.x server stack (amber), avoids evicting the Jersey
// their auth wiring depends on.
val excludeHadoopJersey2Stack = Seq(
  ExclusionRule(organization = "org.glassfish.jersey.core"),
  ExclusionRule(organization = "org.glassfish.jersey.containers"),
  ExclusionRule(organization = "org.glassfish.jersey.inject"),
  ExclusionRule(organization = "org.glassfish.jaxb"),
  ExclusionRule(organization = "com.sun.istack"),
  ExclusionRule(organization = "jakarta.servlet.jsp")
)
val excludeSlf4j = ExclusionRule(organization = "org.slf4j")
val excludeJetty = ExclusionRule(organization = "org.eclipse.jetty")
val excludeJsp = ExclusionRule(organization = "javax.servlet.jsp")
val excludeXmlBind = ExclusionRule(organization = "javax.xml.bind")
val excludeJackson = ExclusionRule(organization = "com.fasterxml.jackson.core")
val excludeJacksonModule = ExclusionRule(organization = "com.fasterxml.jackson.module")
val excludeNetty = ExclusionRule(organization = "io.netty")
val log4jVersion = "2.26.1"
// Iceberg 1.9.2 pairs with parquet 1.15.2, clearing the parquet-avro CVEs
// (CVE-2025-30065, CVE-2025-46762) pulled in transitively by older releases.
val icebergVersion = "1.9.2"

libraryDependencies ++= Seq(
  "org.apache.iceberg" % "iceberg-api" % icebergVersion,
  "org.apache.iceberg" % "iceberg-parquet" % icebergVersion excludeAll(
    excludeJackson,
    excludeJacksonModule
  ),
  "org.apache.iceberg" % "iceberg-core" % icebergVersion excludeAll(
    excludeJackson,
    excludeJacksonModule
  ),
  "org.apache.iceberg" % "iceberg-data" % icebergVersion excludeAll(
    excludeJackson,
    excludeJacksonModule
  ),
  "org.apache.iceberg" % "iceberg-aws" % icebergVersion excludeAll(
    excludeJackson,
    excludeJacksonModule
  ),
  "org.apache.hadoop" % "hadoop-common" % "3.5.0" excludeAll(
    (Seq(
      excludeXmlBind,
      excludeJersey,
      excludeJerseyJsonFork,
      excludeSlf4j,
      excludeJetty,
      excludeJsp,
      excludeJackson,
      excludeJacksonModule
    ) ++ excludeHadoopJersey2Stack): _*
  ),
  // hadoop 3.4 adds a direct netty-all dependency; exclude it — the netty
  // artifacts this module needs are declared explicitly above.
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "3.5.0" excludeAll(
    (Seq(
      excludeXmlBind,
      excludeJersey,
      excludeJerseyJsonFork,
      excludeSlf4j,
      excludeJetty,
      excludeJsp,
      excludeJackson,
      excludeJacksonModule,
      excludeNetty
    ) ++ excludeHadoopJersey2Stack): _*
  ),
  // log4j:log4j is excluded build-wide (root build.sbt) because 1.x is EOL
  // with open CVEs. These log4j 2.x bridges keep hadoop/zookeeper's
  // org.apache.log4j calls working by routing them through the log4j 2 API
  // into slf4j (and on to logback).
  "org.apache.logging.log4j" % "log4j-1.2-api" % log4jVersion,
  "org.apache.logging.log4j" % "log4j-to-slf4j" % log4jVersion,
  "org.postgresql" % "postgresql" % "42.7.10"
)

/////////////////////////////////////////////////////////////////////////////
// Additional Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "com.github.sisyphsu" % "dateparser" % "1.0.11",                    // DateParser
  "com.google.guava" % "guava" % "31.1-jre",                          // Guava
  "org.ehcache" % "sizeof" % "0.4.3",                                 // Ehcache SizeOf
  "org.jgrapht" % "jgrapht-core" % "1.4.0",                           // JGraphT Core
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",          // Scala Logging
  "org.eclipse.jgit" % "org.eclipse.jgit" % "5.13.0.202109080827-r",  // jgit
  // commons-vfs2 pulls hadoop-hdfs-client (for its HDFS provider), which drags in the
  // Hadoop client stack and, via 3.5.0, the Jersey 2.x/JAXB web stack; exclude it here too.
  "org.apache.commons" % "commons-vfs2" % "2.9.0" excludeAll(excludeHadoopJersey2Stack: _*), // for FileResolver throw VFS-related exceptions
  "io.lakefs" % "sdk" % "1.51.0",                                     // for lakeFS api calls
  "com.typesafe" % "config" % "1.4.6",                                 // config reader
  "org.apache.commons" % "commons-jcs3-core" % "3.2",                 // Apache Commons JCS
  "software.amazon.awssdk" % "s3" % "2.29.51" excludeAll(
    ExclusionRule(organization = "io.netty")
  ),
  "software.amazon.awssdk" % "auth" % "2.29.51" excludeAll(
    ExclusionRule(organization = "io.netty")
  ),
  "software.amazon.awssdk" % "regions" % "2.29.51" excludeAll(
    ExclusionRule(organization = "io.netty")
  ),
  "software.amazon.awssdk" % "sts" % "2.29.51" excludeAll(
    ExclusionRule(organization = "io.netty")
  ),
)