name := "texera"
organization := "edu.uci.ics"
version := "0.1-SNAPSHOT"

scalaVersion := "2.12.8"

//To turn on, use: INFO
//To turn off, use: WARNING
scalacOptions ++= Seq("-Xelide-below", "WARNING")
scalacOptions ++= Seq("-feature")

// ensuring no parallel execution of multiple tasks
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

val akkaVersion = "2.6.12"
val hadoopVersion = "3.2.0"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-remote" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-metrics" % akkaVersion,
  "com.typesafe.akka" %% "akka-cluster-tools" % akkaVersion,
  "com.typesafe.akka" %% "akka-multi-node-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "com.typesafe.akka" %% "akka-persistence" % akkaVersion,
  "io.kamon" % "sigar-loader" % "1.6.6-rev002",
  "com.softwaremill.macwire" %% "macros" % "2.3.6" % "provided",
  "com.softwaremill.macwire" %% "macrosakka" % "2.3.6" % "provided",
  "com.softwaremill.macwire" %% "util" % "2.3.6",
  "com.softwaremill.macwire" %% "proxy" % "2.3.6"
)

val excludeHadoopJersey = ExclusionRule(organization = "com.sun.jersey")
val excludeHadoopSlf4j = ExclusionRule(organization = "org.slf4j")
val excludeHadoopJsp = ExclusionRule(organization = "javax.servlet.jsp")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeAll (excludeHadoopJersey, excludeHadoopSlf4j, excludeHadoopJsp),
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion excludeAll (excludeHadoopJersey, excludeHadoopSlf4j, excludeHadoopJsp),
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll (excludeHadoopJersey, excludeHadoopSlf4j, excludeHadoopJsp)
)

libraryDependencies += "org.jgrapht" % "jgrapht-core" % "1.4.0"

// dropwizard web framework
val dropwizardVersion = "1.3.23"
// jersey version should be the same as jersey-server that is contained in dropwizard
val jerseyMultipartVersion = "2.25.1"
val jacksonVersion = "2.12.0"

libraryDependencies ++= Seq(
  "io.dropwizard" % "dropwizard-core" % dropwizardVersion,
  "io.dropwizard" % "dropwizard-client" % dropwizardVersion,
  "com.github.dirkraft.dropwizard" % "dropwizard-file-assets" % "0.0.2",
  "io.dropwizard-bundles" % "dropwizard-redirect-bundle" % "1.0.5",
  "com.liveperson" % "dropwizard-websockets" % "1.3.14",
  "org.glassfish.jersey.media" % "jersey-media-multipart" % jerseyMultipartVersion,
  "com.fasterxml.jackson.module" % "jackson-module-jsonSchema" % jacksonVersion,
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % jacksonVersion
)

libraryDependencies += "com.kjetland" % "mbknor-jackson-jsonschema_2.12" % "1.0.39"

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" classifier "models"

libraryDependencies += "com.twitter" %% "chill-akka" % "0.9.3"
libraryDependencies += "com.twitter" %% "util-core" % "20.9.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.3"
libraryDependencies += "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.22.0"

// https://mvnrepository.com/artifact/com.google.guava/guava
libraryDependencies += "com.google.guava" % "guava" % "29.0-jre"

// https://mvnrepository.com/artifact/org.tukaani/xz
libraryDependencies += "org.tukaani" % "xz" % "1.5"

libraryDependencies += "org.apache.arrow" % "flight-core" % "1.0.1"
libraryDependencies += "org.apache.arrow" % "flight-grpc" % "1.0.1"
libraryDependencies += "io.netty" % "netty-all" % "4.1.48.Final"

libraryDependencies += "org.apache.lucene" % "lucene-core" % "8.7.0"
libraryDependencies += "org.apache.lucene" % "lucene-analyzers-common" % "8.7.0"
libraryDependencies += "org.apache.lucene" % "lucene-queryparser" % "8.7.0"
libraryDependencies += "org.apache.lucene" % "lucene-queries" % "8.7.0"
libraryDependencies += "org.apache.lucene" % "lucene-memory" % "8.7.0"

// https://mvnrepository.com/artifact/mysql/mysql-connector-java
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.19"

// https://mvnrepository.com/artifact/org.postgresql/postgresql
libraryDependencies += "org.postgresql" % "postgresql" % "42.2.18"

// https://mvnrepository.com/artifact/org.jooq/jooq
libraryDependencies += "org.jooq" % "jooq" % "3.14.4"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"

libraryDependencies += "org.scalamock" %% "scalamock" % "4.4.0" % Test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % Test

// https://mvnrepository.com/artifact/com.github.tototoshi/scala-csv
libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6"

// https://mvnrepository.com/artifact/com.konghq/unirest-java
libraryDependencies += "com.konghq" % "unirest-java" % "3.11.11"

// https://mvnrepository.com/artifact/com.github.marianobarrios/lbmq
libraryDependencies += "com.github.marianobarrios" % "lbmq" % "0.5.0"
