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

package org.apache.texera.amber.util

import org.apache.texera.amber.operator.tags.IntegrationTest
import org.apache.texera.common.config.UdfConfig
import org.scalatest.Tag

import java.util.concurrent.TimeUnit
import scala.util.Try

private[amber] object PythonTestSupport {
  val NeedsPythonPackages: Tag = Tag(classOf[IntegrationTest].getName)
  private val PathCandidates = List("python3", "python", "py")

  def configuredPython(): Option[String] = {
    val configured = Some(UdfConfig.pythonPath.trim).filter(_.nonEmpty)
    (configured.toList ++ PathCandidates).distinct.find(runnable)
  }

  def pathPython(): Option[String] = PathCandidates.find(runnable)

  def canImport(python: String, modules: String*): Boolean =
    canImportWithin(python, 60, modules: _*)

  def canImportWithin(python: String, timeoutSeconds: Int, modules: String*): Boolean =
    succeeds(Seq(python, "-c", "import " + modules.mkString(", ")), timeoutSeconds)

  private def runnable(executable: String): Boolean =
    succeeds(Seq(executable, "--version"), 5)

  private def succeeds(command: Seq[String], timeoutSeconds: Int): Boolean =
    Try(new ProcessBuilder(command: _*).redirectErrorStream(true).start()).toOption.exists {
      process =>
        if (process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) process.exitValue() == 0
        else {
          process.destroyForcibly()
          false
        }
    }
}
