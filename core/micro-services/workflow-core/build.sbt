name := "workflow-core"

version := "0.1.0"

scalaVersion := "2.13.12"

enablePlugins(JavaAppPackaging)

semanticdbEnabled := true
semanticdbVersion := scalafixSemanticdb.revision

// to turn on, use: INFO
// to turn off, use: WARNING
scalacOptions ++= Seq("-Xelide-below", "WARNING")

// to check feature warnings
scalacOptions += "-feature"
// to check deprecation warnings
scalacOptions += "-deprecation"
// to check unused imports
scalacOptions += "-Ywarn-unused:imports"

conflictManager := ConflictManager.latestRevision

// ensuring no parallel execution of multiple tasks
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)



/////////////////////////////////////////////////////////////////////////////
// ScalaPB related
// Excluding some proto files:
PB.generate / excludeFilter := "scalapb.proto"

// ScalaPB code generation for .proto files
PB.protocVersion := "3.19.4"


Compile / PB.targets := Seq(
  scalapb.gen(
    singleLineToProtoString = true
  ) -> (sourceManaged in Compile).value
)

// Mark the ScalaPB-generated directory as a generated source root
managedSourceDirectories in Compile += (sourceManaged in Compile).value

libraryDependencies ++= Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)
// For ScalaPB 0.11.x:
libraryDependencies += "com.thesamet.scalapb" %% "scalapb-json4s" % "0.12.0"

// enable protobuf compilation in Test
Test / PB.protoSources += PB.externalSourcePath.value


/////////////////////////////////////////////////////////////////////////////
// Test related
// https://mvnrepository.com/artifact/org.scalamock/scalamock
libraryDependencies += "org.scalamock" %% "scalamock" % "5.2.0" % Test
// https://mvnrepository.com/artifact/ch.vorburger.mariaDB4j/mariaDB4j
libraryDependencies += "ch.vorburger.mariaDB4j" % "mariaDB4j" % "2.4.0" % Test
// https://www.scalatest.org/getting_started_with_fun_suite
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.15" % Test
// JUnit related dependencies
libraryDependencies += "junit" % "junit" % "4.13.2" % Test // JUnit dependency for Java tests
libraryDependencies += "com.novocode" % "junit-interface" % "0.11" % Test // SBT interface for JUnit


val jacksonVersion = "2.15.1"
val jacksonDependencies = Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" % "jackson-module-kotlin" % jacksonVersion % "test",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8" % jacksonVersion % "test",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jsr310" % jacksonVersion % "test",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-joda" % jacksonVersion % "test",
  "com.fasterxml.jackson.module" % "jackson-module-jsonSchema" % jacksonVersion,
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.13" % jacksonVersion,
  // https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-no-ctor-deser
  "com.fasterxml.jackson.module" % "jackson-module-no-ctor-deser" % jacksonVersion
)

libraryDependencies ++= jacksonDependencies

val mongoDbDependencies = Seq(
  // https://mvnrepository.com/artifact/org.mongodb/mongodb-driver-sync
  "org.mongodb" % "mongodb-driver-sync" % "5.0.0",
  // https://mvnrepository.com/artifact/org.apache.commons/commons-jcs3-core
  "org.apache.commons" % "commons-jcs3-core" % "3.2"
)

libraryDependencies ++= mongoDbDependencies

// https://mvnrepository.com/artifact/com.github.sisyphsu/dateparser
libraryDependencies += "com.github.sisyphsu" % "dateparser" % "1.0.11"

// Add Guava library
libraryDependencies += "com.google.guava" % "guava" % "31.1-jre"

// https://mvnrepository.com/artifact/org.ehcache/sizeof
libraryDependencies += "org.ehcache" % "sizeof" % "0.4.3"

// https://mvnrepository.com/artifact/org.jgrapht/jgrapht-core
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0"

// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"

