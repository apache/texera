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

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.pybuilder.PythonReflectionTextUtils.truncateBlock
import org.apache.texera.amber.pybuilder.PythonReflectionUtils
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent
import java.util.concurrent.TimeUnit
import scala.util.Try

/**
  * Regression tests for validation pipeline used for PythonOperatorDescriptor codegen.
  *
  * What this suite checks:
  *  1) Code generation must not leak raw invalid text from @JsonProperty string values into the emitted Python.
  *  2) The emitted Python should pass a basic `py_compile` sanity check under an isolated interpreter.
  *
  * Notes:
  *  - "RawInvalid" is a marker chosen to be very unlikely to appear in real code.
  *  - We only scan under AcceptPackages to keep the suite fast and avoid pulling in unrelated classes.
  */
final class PythonCodeRawInvalidTextSpec extends AnyFunSuite {

  // Scala literal "\\!." is the 3-char string: \!.
  private val RawInvalid: String = "\\!."
  private val MaxDepth: Int = 3
  private val AcceptPackages: Seq[String] = Seq("org.apache.texera.amber.operator")

  /**
    * Runs `python -m py_compile` on the provided source, using an isolated interpreter invocation.
    *
    * Isolation flags:
    *  - -I : isolate (ignore user site-packages / env)
    *  - -S : don't import site
    *  - -B : don't write .pyc files
    *
    * @return Right(()) on success, Left(errorMessage) on failure (including timeout).
    */
  private def pyCompile(pythonExecutable: String, pythonSource: String): Either[String, Unit] = {
    val tempFile = Files.createTempFile("texera_py_compile_", ".py")
    try {
      Files.write(tempFile, pythonSource.getBytes(StandardCharsets.UTF_8))

      val processBuilder =
        new ProcessBuilder(
          pythonExecutable,
          "-I",
          "-S",
          "-B",
          "-m",
          "py_compile",
          tempFile.toString
        )
      // Merge stderr into stdout to keep a single combined output stream for easy reporting.
      processBuilder.redirectErrorStream(true)

      val processStartEither = Try(processBuilder.start()).toEither.left.map { thrown =>
        s"Could not start python executable '$pythonExecutable': ${thrown.getClass.getName}: ${Option(thrown.getMessage)
          .getOrElse("")}"
      }

      processStartEither.flatMap { process =>
        val didFinish = process.waitFor(30, concurrent.TimeUnit.SECONDS)
        if (!didFinish) {
          process.destroyForcibly()
          Left("py_compile timed out after 30s (process was killed)")
        } else {
          val combinedOutput =
            Try(new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8))
              .getOrElse("")
              .trim
          val exitCode = process.exitValue()
          if (exitCode == 0) Right(())
          else {
            val clippedOutput =
              if (combinedOutput.nonEmpty)
                truncateBlock(combinedOutput, maxLines = 40, maxChars = 8000)
              else "(no output)"
            Left(s"py_compile failed (exit=$exitCode)\nOutput:\n$clippedOutput")
          }
        }
      }
    } finally {
      Try(Files.deleteIfExists(tempFile))
      ()
    }
  }

  /**
    * Loads the Python executable path from configuration, with fallbacks.
    *
    * Lookup strategy:
    *  1) Try parsing udf.conf from resources and resolving it.
    *  2) Fall back to ConfigFactory.load().
    *  3) Read python.path, trim, and ensure it's non-empty.
    *  4) If missing or invalid, fall back to "python3", then "python", then "py"
    *     (validated by running --version).
    */
  private def loadPythonExeFromUdfConf(): Option[String] = {

    def fromConfig: Option[String] = {
      val configOpt =
        Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
          .orElse(Try(ConfigFactory.load()).toOption)

      configOpt
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)
    }

    def isRunnable(exe: String): Boolean = {
      val pTry = Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start())
      pTry.toOption.exists { p =>
        val finished = p.waitFor(5, TimeUnit.SECONDS)
        if (!finished) { p.destroyForcibly(); false }
        else p.exitValue() == 0
      }
    }

    val candidates =
      fromConfig.toList ++ List("python3", "python", "py")

    candidates.distinct.find(isRunnable)
  }

  test(
    "PythonOperatorDescriptor.generatePythonCode should not contain raw invalid JsonProperty Strings"
  ) {
    val classLoader = Thread.currentThread().getContextClassLoader

    val descriptorCandidates =
      PythonReflectionUtils
        .scanCandidates(
          base = classOf[PythonOperatorDescriptor],
          acceptPackages = AcceptPackages,
          classLoader = classLoader
        )
        .map(_.asInstanceOf[Class[_ <: PythonOperatorDescriptor]])
        .sortBy(_.getName)

    if (descriptorCandidates.isEmpty) {
      fail(
        s"No implementations of ${classOf[PythonOperatorDescriptor].getName} were found. " +
          s"Check acceptPackages() / test classpath / module wiring."
      )
    }

    val total = descriptorCandidates.size
    var ok = 0
    var checked = 0

    val allFindings = descriptorCandidates.flatMap { descriptorClass =>
      checked += 1
      val findings =
        PythonReflectionUtils.checkDescriptor(
          descriptorClass,
          rawInvalidText = RawInvalid,
          maxDepth = MaxDepth
        )

      if (findings.isEmpty) {
        ok += 1
        println(s"[raw-invalid OK $ok/$total | checked $checked/$total] ${descriptorClass.getName}")
      }

      findings
    }

    println(s"[raw-invalid SUMMARY] ok=$ok/$total")

    if (allFindings.nonEmpty) {
      fail(PythonReflectionUtils.renderReport(allFindings, total = total))
    }
  }

  test("PythonOperatorDescriptor.generatePythonCode should py_compile under isolated Python") {
    val pythonExeOpt = loadPythonExeFromUdfConf()
    if (pythonExeOpt.isEmpty) {
      fail(
        "python.path not found in udf.conf (or application.conf). Configure python.path to enable this test."
      )
    }
    val pythonExecutable = pythonExeOpt.get
    val classLoader = Thread.currentThread().getContextClassLoader

    val descriptorCandidates =
      PythonReflectionUtils
        .scanCandidates(
          base = classOf[PythonOperatorDescriptor],
          acceptPackages = AcceptPackages,
          classLoader = classLoader
        )
        .map(_.asInstanceOf[Class[_ <: PythonOperatorDescriptor]])
        .sortBy(_.getName)

    if (descriptorCandidates.isEmpty) {
      fail(
        s"No implementations of ${classOf[PythonOperatorDescriptor].getName} were found. " +
          s"Check acceptPackages() / test classpath / module wiring."
      )
    }

    val total = descriptorCandidates.size
    var ok = 0
    var checked = 0

    val allFindings = descriptorCandidates.flatMap { descriptorClass =>
      checked += 1

      val checkResult =
        PythonReflectionUtils.checkDescriptorWithCode(
          descriptorClass,
          rawInvalidText = RawInvalid,
          maxDepth = MaxDepth
        )

      val pyCompileFindings = checkResult.code.toSeq.flatMap { generatedCode =>
        pyCompile(pythonExecutable, generatedCode) match {
          case Left(errorMessage) =>
            Seq(PythonReflectionUtils.Finding(descriptorClass.getName, "py-compile", errorMessage))
          case Right(()) => Nil
        }
      }

      val findings = checkResult.findings ++ pyCompileFindings

      if (findings.isEmpty && checkResult.code.nonEmpty) {
        ok += 1
        println(s"[py-compile OK $ok/$total | checked $checked/$total] ${descriptorClass.getName}")
      }

      findings
    }

    println(s"[py-compile SUMMARY] ok=$ok/$total")

    if (allFindings.nonEmpty) {
      fail(PythonReflectionUtils.renderReport(allFindings, total = total))
    }
  }

}
