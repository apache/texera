/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import sbt._

import scala.io.Source

/**
 * Single source of truth for JDK 17+ JVM flags. Reads non-comment lines
 * from .jvmopts and exposes them as Seq[String], so the same flag list
 * reaches every JVM the build can launch:
 *   - sbt's own JVM           (sbt launcher reads .jvmopts directly)
 *   - forked test JVMs        (build.sbt -> Test / javaOptions)
 *   - sbt-native-packager     (build.sbt -> Universal / javaOptions, with
 *     bin/<svc> launchers      "-J" prefix per launcher convention)
 *   - IntelliJ Application    (.run/[svc].run.xml carries
 *     run configs               VM_PARAMETERS = @.jvmopts; JDK 9+
 *                               argfile expansion at JVM start)
 *
 * Modeled after Pekko's project/JdkOptions.scala. The JDK version gate
 * keeps the build self-consistent on JDK 8 (where --add-opens does not
 * exist) even though Texera ships JDK 17 only.
 */
object JdkOptions {

  /** JVM flags from .jvmopts at the build root, or empty on JDK <9. */
  def jvmFlags(baseDir: File): Seq[String] =
    if (jdkSpecVersion < 9) Seq.empty
    else readJvmopts(baseDir / ".jvmopts")

  private def jdkSpecVersion: Int = {
    val raw = sys.props.getOrElse("java.specification.version", "0")
    val s = if (raw.startsWith("1.")) raw.drop(2) else raw
    s.takeWhile(_.isDigit) match {
      case ""    => 0
      case digit => digit.toInt
    }
  }

  private def readJvmopts(f: File): Seq[String] =
    if (!f.exists()) Seq.empty
    else {
      val src = Source.fromFile(f)
      try src.getLines()
        .map(_.trim)
        .filter(l => l.nonEmpty && !l.startsWith("#"))
        .toList
      finally src.close()
    }
}
