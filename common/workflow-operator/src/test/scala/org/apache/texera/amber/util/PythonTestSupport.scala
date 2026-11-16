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
import org.scalatest.Assertions.fail
import org.scalatest.Tag

import java.io.InputStream
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

private[amber] object PythonTestSupport {
  val NeedsPythonPackages: Tag = Tag(classOf[IntegrationTest].getName)
  private val PathCandidates = List("python3", "python", "py")

  /** The first interpreter that runs: `python.path` from udf.conf (set by `UDF_PYTHON_PATH`), then
    * python3, python and py from the PATH.
    */
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

  // Nothing reads the child's output, and a pipe nobody reads blocks the child once it fills, so
  // the output is discarded.
  private def succeeds(command: Seq[String], timeoutSeconds: Int): Boolean =
    Try(
      new ProcessBuilder(command: _*)
        .redirectErrorStream(true)
        .redirectOutput(ProcessBuilder.Redirect.DISCARD)
        .start()
    ).toOption.exists { process =>
      if (process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) process.exitValue() == 0
      else {
        process.destroyForcibly()
        false
      }
    }

  private def readDefault(stream: InputStream): String =
    Source.fromInputStream(stream).mkString

  def readUtf8(stream: InputStream): String =
    Source.fromInputStream(stream, "UTF-8").mkString

  def readUtf8WithReplacement(stream: InputStream): String =
    new String(stream.readAllBytes(), UTF_8)

  def runSource(
      python: String,
      prefix: String,
      code: String,
      timeoutSeconds: Int = 120,
      environment: Map[String, String] = Map.empty,
      readOutput: InputStream => String = readDefault
  ): (Int, String) = {
    val script = Files.createTempFile(prefix, ".py")
    script.toFile.deleteOnExit()
    Files.write(script, code.getBytes(UTF_8))
    runScript(python, script, timeoutSeconds, environment = environment, readOutput = readOutput)
  }

  def runScript(
      python: String,
      script: Path,
      timeoutSeconds: Int = 120,
      directory: Option[Path] = None,
      environment: Map[String, String] = Map.empty,
      readOutput: InputStream => String = readDefault
  ): (Int, String) = {
    val builder = new ProcessBuilder(python, script.toString).redirectErrorStream(true)
    directory.foreach(path => builder.directory(path.toFile))
    environment.foreach { case (key, value) => builder.environment().put(key, value) }
    val process = builder.start()
    val output = readOutput(process.getInputStream)
    process.waitFor(timeoutSeconds, TimeUnit.SECONDS)
    (process.exitValue(), output)
  }

  def withDriver[A](
      python: String,
      modulePrefix: String,
      driverPrefix: String,
      module: => String,
      driver: String,
      timeoutMessage: String,
      bestEffortCleanup: Boolean = true
  )(check: (Int, String) => A): A = {
    val moduleFile = Files.createTempFile(modulePrefix, ".py")
    val driverFile = Files.createTempFile(driverPrefix, ".py")
    try {
      Files.write(moduleFile, module.getBytes(UTF_8))
      Files.write(driverFile, driver.getBytes(UTF_8))
      val process = new ProcessBuilder(python, driverFile.toString, moduleFile.toString)
        .redirectErrorStream(true)
        .start()
      if (!process.waitFor(120, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        fail(timeoutMessage)
      }
      val output = readUtf8WithReplacement(process.getInputStream)
      check(process.exitValue(), output)
    } finally {
      if (bestEffortCleanup) {
        Try(Files.deleteIfExists(moduleFile))
        Try(Files.deleteIfExists(driverFile))
      } else {
        Files.deleteIfExists(moduleFile)
        Files.deleteIfExists(driverFile)
      }
    }
  }
  def copyScriptResource(resource: InputStream, prefix: String): Path = {
    val source = Source.fromInputStream(resource, "UTF-8")
    val body =
      try source.mkString
      finally source.close()
    val file = Files.createTempFile(prefix, ".py")
    Files.write(file, body.getBytes(UTF_8))
    file.toFile.deleteOnExit()
    file
  }

  def withProbe[A](
      python: String,
      script: Path,
      prefix: String,
      timeoutSeconds: Int,
      input: => Array[Byte]
  )(check: (Int, String, String) => A): A = {
    val outFile = Files.createTempFile(prefix + "_out", ".json")
    val errFile = Files.createTempFile(prefix + "_err", ".txt")
    try {
      val process = new ProcessBuilder(python, script.toString)
        .redirectOutput(outFile.toFile)
        .redirectError(errFile.toFile)
        .start()
      process.getOutputStream.write(input)
      process.getOutputStream.close()
      if (!process.waitFor(timeoutSeconds, TimeUnit.SECONDS)) {
        process.destroyForcibly()
        fail("probe timed out")
      }
      val out = new String(Files.readAllBytes(outFile), UTF_8)
      val err = new String(Files.readAllBytes(errFile), UTF_8)
      check(process.exitValue(), out, err)
    } finally {
      Files.deleteIfExists(outFile)
      Files.deleteIfExists(errFile)
    }
  }
}
