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

addSbtPlugin("org.scalameta" % "sbt-scalafmt" % "2.4.2")
addSbtPlugin("ch.epfl.scala" % "sbt-scalafix" % "0.14.7")
// Coverage instrumentation; emits jacoco.xml that Codecov consumes.
// JaCoCo (vs scoverage) works on JVM bytecode, so it does not need a
// per-Scala-version compiler plugin — scalac-scoverage-plugin only
// publishes up to 2.13.16, but Texera builds on 2.13.18.
addSbtPlugin("com.github.sbt" % "sbt-jacoco" % "3.5.0")
// License reporting for dependency compliance auditing
// See: https://github.com/sbt/sbt-license-report
addSbtPlugin("com.github.sbt" % "sbt-license-report" % "1.7.0")

libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.11.20"
addSbtPlugin("com.github.sbt" % "sbt-native-packager" % "1.11.7")
// for scalapb code gen
addSbtPlugin("org.typelevel" % "sbt-fs2-grpc" % "2.11.0")

// JOOQ dependencies for code generation
libraryDependencies ++= Seq(
  "org.jooq" % "jooq-codegen" % "3.16.23",
  "com.typesafe" % "config" % "1.4.9",
  // Pinned to 42.7.4 (build-time codegen driver only, not bundled). pgjdbc >= 42.7.5
  // returns JDBC-spec uppercase metadata column labels (KEY_SEQ) that jOOQ 3.16.x's
  // PostgresDatabase.loadForeignKeys can't read (it looks up lowercase key_seq),
  // breaking JOOQ code generation. Fixed only in jOOQ 3.20+ (jOOQ/jOOQ#17873); until
  // the codegen jOOQ is upgraded, keep this driver at the last compatible release.
  "org.postgresql" % "postgresql" % "42.7.4"
)
