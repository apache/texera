name := "texera"
organization := "edu.uci.ics"
version := "0.1-SNAPSHOT"

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

// temp fix for the netty dependency issue
// https://github.com/coursier/coursier/issues/2016
ThisBuild / useCoursier := false

// add python as an additional source
Compile / unmanagedSourceDirectories += baseDirectory.value / "src" / "main" / "python"

// Excluding some proto files:
PB.generate / excludeFilter := "scalapb.proto"

/////////////////////////////////////////////////////////////////////////////
// Akka related
val akkaVersion = "2.8.3"
val akkaDependencies = Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "io.kamon" % "sigar-loader" % "1.6.6-rev002",
  "com.softwaremill.macwire" %% "macros" % "2.5.8" % Provided,
  "com.softwaremill.macwire" %% "macrosakka" % "2.5.8" % Provided,
  "com.softwaremill.macwire" %% "util" % "2.5.8",
  "com.softwaremill.macwire" %% "proxy" % "2.5.8",
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)

// dropwizard web framework

/////////////////////////////////////////////////////////////////////////////
// DropWizard server related
val dropwizardVersion = "1.3.23"
// jersey version should be the same as jersey-server that is contained in dropwizard
val jerseyMultipartVersion = "2.25.1"

val dropwizardDependencies = Seq(
  "io.dropwizard" % "dropwizard-core" % dropwizardVersion,
  "io.dropwizard" % "dropwizard-client" % dropwizardVersion,
  "io.dropwizard" % "dropwizard-auth" % dropwizardVersion,
  // https://mvnrepository.com/artifact/com.github.toastshaman/dropwizard-auth-jwt
  "com.github.toastshaman" % "dropwizard-auth-jwt" % "1.1.2-0",
  "com.github.dirkraft.dropwizard" % "dropwizard-file-assets" % "0.0.2",
  "io.dropwizard-bundles" % "dropwizard-redirect-bundle" % "1.0.5",
  "com.liveperson" % "dropwizard-websockets" % "1.3.14",
  "org.glassfish.jersey.media" % "jersey-media-multipart" % jerseyMultipartVersion,
  // https://mvnrepository.com/artifact/commons-io/commons-io
  "commons-io" % "commons-io" % "2.15.1"
)


val jacksonVersion = "2.15.1"
val mbknorJacksonJsonSchemaDependencies = Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
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
// Lucene related
val luceneVersion = "8.7.0"
val luceneDependencies = Seq(
  "org.apache.lucene" % "lucene-core" % luceneVersion,
  "org.apache.lucene" % "lucene-queryparser" % luceneVersion,
  "org.apache.lucene" % "lucene-queries" % luceneVersion,
  "org.apache.lucene" % "lucene-memory" % luceneVersion
)

/////////////////////////////////////////////////////////////////////////////
// Hadoop related
val hadoopVersion = "3.3.3"
val excludeHadoopJersey = ExclusionRule(organization = "com.sun.jersey")
val excludeHadoopSlf4j = ExclusionRule(organization = "org.slf4j")
val excludeHadoopJetty = ExclusionRule(organization = "org.eclipse.jetty")
val excludeHadoopJsp = ExclusionRule(organization = "javax.servlet.jsp")
val hadoopDependencies = Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeAll (excludeHadoopJersey, excludeHadoopSlf4j, excludeHadoopJsp, excludeHadoopJetty)
)

/////////////////////////////////////////////////////////////////////////////
// Google Service related
val googleServiceDependencies = Seq(
  "com.google.oauth-client" % "google-oauth-client-jetty" % "1.34.1" exclude ("com.google.guava", "guava"),
  "com.google.api-client" % "google-api-client" % "2.2.0" exclude ("com.google.guava", "guava"),
  "com.google.apis" % "google-api-services-sheets" % "v4-rev612-1.25.0" exclude ("com.google.guava", "guava"),
  "com.google.apis" % "google-api-services-drive" % "v3-rev197-1.25.0" exclude ("com.google.guava", "guava"),
  "com.sun.mail" % "javax.mail" % "1.6.2"
)

/////////////////////////////////////////////////////////////////////////////
// Arrow related
val arrowVersion = "14.0.1"
val arrowDependencies = Seq(
  // https://mvnrepository.com/artifact/org.apache.arrow/flight-grpc
  "org.apache.arrow" % "flight-grpc" % arrowVersion,
  // https://mvnrepository.com/artifact/org.apache.arrow/flight-core
  "org.apache.arrow" % "flight-core" % arrowVersion
)

/////////////////////////////////////////////////////////////////////////////
// MongoDB related
val mongoDbDependencies = Seq(
  // https://mvnrepository.com/artifact/org.mongodb/mongodb-driver-sync
  "org.mongodb" % "mongodb-driver-sync" % "5.0.0",
  // https://mvnrepository.com/artifact/org.apache.commons/commons-jcs3-core
  "org.apache.commons" % "commons-jcs3-core" % "3.2"
)

libraryDependencies ++= akkaDependencies
libraryDependencies ++= luceneDependencies
libraryDependencies ++= dropwizardDependencies
libraryDependencies ++= mbknorJacksonJsonSchemaDependencies
libraryDependencies ++= arrowDependencies
libraryDependencies ++= googleServiceDependencies
libraryDependencies ++= mongoDbDependencies
libraryDependencies ++= hadoopDependencies

/////////////////////////////////////////////////////////////////////////////
// protobuf related
// run the following with sbt to have protobuf codegen

PB.protocVersion := "3.19.4"

enablePlugins(Fs2Grpc)

fs2GrpcOutputPath := (Compile / sourceDirectory).value / "scalapb"

Compile / PB.targets := Seq(
  scalapb.gen(
    singleLineToProtoString = true
  ) -> (Compile / sourceDirectory).value / "scalapb",
  // let fs2 compile grpc-related proto, skip other protos in fs2 compilation pipeline.
  scalapbCodeGenerators.value(1)
)

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

/////////////////////////////////////////////////////////////////////////////
// Workflow version control related
// https://mvnrepository.com/artifact/com.flipkart.zjsonpatch/zjsonpatch
libraryDependencies += "com.flipkart.zjsonpatch" % "zjsonpatch" % "0.4.13"

/////////////////////////////////////////////////////////////////////////////
// Uncategorized

// https://mvnrepository.com/artifact/io.reactivex.rxjava3/rxjava
libraryDependencies += "io.reactivex.rxjava3" % "rxjava" % "3.1.6"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.5.4"

// https://mvnrepository.com/artifact/com.typesafe.scala-logging/scala-logging
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"

// https://mvnrepository.com/artifact/org.scalactic/scalactic
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.15"

// https://mvnrepository.com/artifact/com.github.tototoshi/scala-csv
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.10"

// https://mvnrepository.com/artifact/com.univocity/univocity-parsers
libraryDependencies += "com.univocity" % "univocity-parsers" % "2.9.1"

// https://mvnrepository.com/artifact/com.konghq/unirest-java
libraryDependencies += "com.konghq" % "unirest-java" % "3.14.2"

// https://mvnrepository.com/artifact/com.github.marianobarrios/lbmq
libraryDependencies += "com.github.marianobarrios" % "lbmq" % "0.6.0"

// https://mvnrepository.com/artifact/io.github.redouane59.twitter/twittered
libraryDependencies += "io.github.redouane59.twitter" % "twittered" % "2.21"

// https://mvnrepository.com/artifact/org.jooq/jooq
libraryDependencies += "org.jooq" % "jooq" % "3.14.16"

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.33"

// https://mvnrepository.com/artifact/org.jgrapht/jgrapht-core
libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0"

// https://mvnrepository.com/artifact/edu.stanford.nlp/stanford-corenlp
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.5.4"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "4.5.4" classifier "models"

// https://mvnrepository.com/artifact/com.twitter/chill-akka
libraryDependencies += "com.twitter" %% "chill-akka" % "0.10.0"

// https://mvnrepository.com/artifact/com.twitter/util-core
libraryDependencies += "com.twitter" %% "util-core" % "22.12.0"

// https://mvnrepository.com/artifact/com.typesafe.play/play-json
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.4"

// https://mvnrepository.com/artifact/org.fusesource.leveldbjni/leveldbjni-all
libraryDependencies += "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"

// https://mvnrepository.com/artifact/com.github.nscala-time/nscala-time
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.32.0"

// https://mvnrepository.com/artifact/com.google.guava/guava
libraryDependencies += "com.google.guava" % "guava" % "29.0-jre"

// https://mvnrepository.com/artifact/org.tukaani/xz
libraryDependencies += "org.tukaani" % "xz" % "1.9"

// https://mvnrepository.com/artifact/org.jasypt/jasypt
libraryDependencies += "org.jasypt" % "jasypt" % "1.9.3"

// Jgit library for tracking operator version
// https://mvnrepository.com/artifact/org.eclipse.jgit/org.eclipse.jgit
libraryDependencies += "org.eclipse.jgit" % "org.eclipse.jgit" % "5.13.0.202109080827-r"

// https://mvnrepository.com/artifact/org.ehcache/sizeof
libraryDependencies += "org.ehcache" % "sizeof" % "0.4.3"

// https://mvnrepository.com/artifact/org.mindrot/jbcrypt
libraryDependencies += "org.mindrot" % "jbcrypt" % "0.4"

// https://mvnrepository.com/artifact/com.github.sisyphsu/dateparser
libraryDependencies += "com.github.sisyphsu" % "dateparser" % "1.0.11"

// https://mvnrepository.com/artifact/org.apache.commons/commons-vfs2
libraryDependencies += "org.apache.commons" % "commons-vfs2" % "2.9.0"

// For supporting MultiDict
// https://mvnrepository.com/artifact/org.scala-lang.modules/scala-collection-contrib
libraryDependencies += "org.scala-lang.modules" %% "scala-collection-contrib" % "0.3.0"

// For supporting deepcopy
// https://mvnrepository.com/artifact/io.github.kostaskougios/cloning
libraryDependencies += "io.github.kostaskougios" % "cloning" % "1.10.3"


