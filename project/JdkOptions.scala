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
 * This file reads JDK 17+ JVM flags from .jvmopts so every JVM the build
 * launches shares one flag list.
 *
 * Modeled on Pekko's project/JdkOptions.scala. The JDK 8 gate is
 * defensive: --add-opens does not exist before JDK 9, so jvmFlags
 * stays empty there even though Texera ships JDK 17 only.
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
