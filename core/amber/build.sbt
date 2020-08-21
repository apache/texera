
name := "Amber"

version := "0.1"

scalaVersion := "2.12.8"

//To turn on, use: INFO
//To turn off, use: WARNING
scalacOptions ++= Seq("-Xelide-below", "WARNING")
scalacOptions ++= Seq("-feature")

val akkaVersion = "2.5.24"
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
  "com.chuusai" %% "shapeless" % "2.3.3")

val excludeHadoopJersey = ExclusionRule(organization = "com.sun.jersey")
val excludeHadoopSlf4j = ExclusionRule(organization = "org.slf4j")
val excludeHadoopJsp = ExclusionRule(organization = "javax.servlet.jsp")

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion excludeAll(excludeHadoopJersey, excludeHadoopSlf4j, excludeHadoopJsp),
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion excludeAll(excludeHadoopJersey, excludeHadoopSlf4j, excludeHadoopJsp),
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion excludeAll(excludeHadoopJersey, excludeHadoopSlf4j, excludeHadoopJsp),
)

// dropwizard webframework
val dropwizardVersion = "1.3.23"
// jersey version should be the same as jersey-server that is contained in dropwizard
val jerseyMultipartVersion = "2.25.1"
val jacksonVersion = "2.9.10"

libraryDependencies ++= Seq(
  "io.dropwizard" % "dropwizard-core" % dropwizardVersion,
  "io.dropwizard" % "dropwizard-client" % dropwizardVersion,

  "com.github.dirkraft.dropwizard" % "dropwizard-file-assets" % "0.0.2",
  "io.dropwizard-bundles" % "dropwizard-redirect-bundle" % "1.0.5",
  "com.liveperson" % "dropwizard-websockets" % "1.3.14",
  "org.glassfish.jersey.media" % "jersey-media-multipart" % jerseyMultipartVersion,
  "com.fasterxml.jackson.module" % "jackson-module-jsonSchema" % jacksonVersion,
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.12" % jacksonVersion,
)

libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2"
libraryDependencies += "edu.stanford.nlp" % "stanford-corenlp" % "3.9.2" classifier "models"

libraryDependencies += "com.twitter" %% "chill-akka" % "0.9.3"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.7.3"
libraryDependencies += "org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"
libraryDependencies += "com.github.nscala-time" %% "nscala-time" % "2.22.0"

// https://mvnrepository.com/artifact/com.google.guava/guava
libraryDependencies += "com.google.guava" % "guava" % "29.0-jre"

// https://mvnrepository.com/artifact/org.tukaani/xz
libraryDependencies += "org.tukaani" % "xz" % "1.5"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.8" % Test

