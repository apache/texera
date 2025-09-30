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

name := "dao"
organization := "edu.uci.ics"
version := "1.0.0"
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
// JOOQ Code Generation
/////////////////////////////////////////////////////////////////////////////

// Define JOOQ generation task
lazy val jooqGenerate = taskKey[Seq[File]]("Generate JOOQ sources")

jooqGenerate := {
  val log = streams.value.log
  log.info("Generating JOOQ classes...")

  try {
    import com.typesafe.config.{Config, ConfigFactory, ConfigParseOptions}
    import org.jooq.codegen.GenerationTool
    import org.jooq.meta.jaxb.{Configuration, Jdbc}
    import java.nio.file.{Files, Path}
    import java.io.File

    // Load jOOQ configuration XML (absolute path from DAO project)
    val jooqXmlPath: Path =
      baseDirectory.value.toPath.resolve("src").resolve("main").resolve("resources").resolve("jooq-conf.xml")
    val jooqConfig: Configuration = GenerationTool.load(Files.newInputStream(jooqXmlPath))

    // Load storage.conf from the config project
    val storageConfPath: Path = baseDirectory.value.toPath
      .getParent
      .resolve("config")
      .resolve("src")
      .resolve("main")
      .resolve("resources")
      .resolve("storage.conf")

    val conf: Config = ConfigFactory
      .parseFile(
        new File(storageConfPath.toString),
        ConfigParseOptions.defaults().setAllowMissing(false)
      )
      .resolve()

    // Extract JDBC configuration
    val jdbcConfig = conf.getConfig("storage.jdbc")

    val jooqJdbcConfig = new Jdbc
    jooqJdbcConfig.setDriver("org.postgresql.Driver")
    // Skip all the query params, otherwise it will omit the "texera_db." prefix on the field names.
    jooqJdbcConfig.setUrl(jdbcConfig.getString("url").split('?').head)
    jooqJdbcConfig.setUsername(jdbcConfig.getString("username"))
    jooqJdbcConfig.setPassword(jdbcConfig.getString("password"))

    jooqConfig.setJdbc(jooqJdbcConfig)

    // Generate the code
    GenerationTool.generate(jooqConfig)
    log.info("JOOQ code generation completed successfully")

    // Return the generated files
    val generatedDir = baseDirectory.value / "src" / "main" / "scala" / "edu" / "uci" / "ics" / "texera" / "dao" / "jooq" / "generated"
    if (generatedDir.exists()) {
      (generatedDir ** "*.java").get ++ (generatedDir ** "*.scala").get
    } else {
      Seq.empty
    }
  } catch {
    case e: Exception =>
      log.warn(s"JOOQ code generation failed: ${e.getMessage}")
      log.warn("Continuing compilation with existing generated files...")
      Seq.empty
  }
}

// Add JOOQ generation to source generators
Compile / sourceGenerators += jooqGenerate


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
  "io.zonky.test" % "embedded-postgres" % "2.1.0" % Test            // For mock postgres DB
)

/////////////////////////////////////////////////////////////////////////////
// Jooq-related Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "org.jooq" % "jooq" % "3.16.23",
  "org.jooq" % "jooq-codegen" % "3.16.23"
)

/////////////////////////////////////////////////////////////////////////////
// Additional Dependencies
/////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "org.postgresql" % "postgresql" % "42.7.4",
  "com.typesafe" % "config" % "1.4.3"    // config reader
)
