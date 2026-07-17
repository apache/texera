/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//////////////////////////////////////////////////////////////////////////////
// Compilation
//////////////////////////////////////////////////////////////////////////////

scalacOptions += "-Ymacro-annotations"

// Scala compiler options (mirrors the other common modules; `-Ywarn-unused:imports`
// is required by the scalafix RemoveUnused rule that CI runs via scalafixAll).
Compile / scalacOptions ++= Seq(
  "-Xelide-below",
  "WARNING",
  "-feature",
  "-deprecation",
  "-Ywarn-unused:imports"
)

//////////////////////////////////////////////////////////////////////////////
// Dependencies
//////////////////////////////////////////////////////////////////////////////

libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)

// Arrow 19's transitive deps (via WorkflowOperator -> WorkflowCore) pull
// jackson-databind past the 2.18 line that jackson-module-scala is pinned to;
// force the Jackson core family back so the Scala module can initialize in
// tests (else Test aborts with "Scala module 2.18.x requires Jackson Databind
// version >= 2.18.0 and < 2.19.0"). Mirrors common/workflow-core/build.sbt.
val jacksonVersion = "2.18.8"
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion
)
