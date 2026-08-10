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
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.apache.texera.amber.pybuilder.PythonReflectionTextUtils.truncateBlock
import org.apache.texera.amber.pybuilder.PythonReflectionUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.apache.texera.amber.util.python.PythonWorkerPool
import org.scalatest.Tag
import org.scalatest.funsuite.AnyFunSuite

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

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

  /** Budget for one whole fanned-out pass over every descriptor. Deliberately far
    * above a real run (well under a second), so it only ever fires on a hang.
    */
  private val PassTimeout: FiniteDuration = 10.minutes

  /** Runs the given work concurrently and returns the results in submission
    * order, rethrowing the first failure so it fails the test. Sized to the pool
    * so the threads match the workers available to serve them.
    *
    * Daemon threads: a task parked on a subprocess pipe answers no interrupt, so
    * `shutdownNow` need not end it, and a non-daemon one left there would hold the
    * JVM — and the build — open after [[PassTimeout]] has already failed the test.
    */
  private def awaitAll[T](work: Seq[() => T]): Seq[T] = {
    val threads = Executors.newFixedThreadPool(
      PythonWorkerPool.maxWorkers,
      (r: Runnable) => {
        val t = new Thread(r, "py-compile-check")
        t.setDaemon(true)
        t
      }
    )
    try {
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutorService(threads)
      Await.result(Future.sequence(work.map(w => Future(w()))), PassTimeout)
    } finally threads.shutdownNow()
  }

  /** Checks the pooled path could not serve. Not a failure — the spawn each one
    * fell back to is the pre-pool behavior — but a number the summary has to
    * carry, since a check passing says nothing about which path answered it.
    */
  private val spawnFallbacks = new AtomicInteger(0)

  /** Syntax-checks one generated module, through a pooled worker when available.
    *
    * The worker is launched with the same `-I -S` isolation the one-shot path
    * uses, so what the check accepts is unchanged; it just stops paying an
    * interpreter boot — the whole cost of a check whose real work is under a
    * millisecond — once per descriptor. A worker the pool cannot give out, or
    * loses mid-job, falls back to the spawn, so behavior is never worse than
    * before the pool.
    */
  private def syntaxCheck(
      pythonExecutable: String,
      pythonSource: String,
      descriptorName: String
  ): Either[String, Unit] = {
    def viaPool: Either[String, Unit] = {
      val request = objectMapper.createObjectNode()
      request.put("source", pythonSource)
      request.put("name", s"$descriptorName.py")
      val outcome = PythonWorkerPool.run(
        resourcePath = "/python/py_compile_worker.py",
        launchArgs = Seq.empty,
        pythonExe = pythonExecutable,
        request = request,
        interpreterArgs = Seq("-I", "-S")
      )
      if (outcome.exit == 0) Right(())
      else {
        val output = if (outcome.stderr.trim.nonEmpty) outcome.stderr.trim else "(no output)"
        Left(
          s"py_compile failed (exit=${outcome.exit})\nOutput:\n" +
            truncateBlock(output, maxLines = 40, maxChars = 8000)
        )
      }
    }

    if (PythonWorkerPool.enabled) {
      try viaPool
      catch {
        // Anything the pooled path throws leaves the spawn as the answer, which is
        // what makes it never worse than before: not only a worker that died
        // mid-job, but equally one the pool could not hand out at all. Those
        // arrive as WorkerDiedException; NonFatal also covers the steps outside
        // that contract, such as materializing the worker script. Counted, so a
        // run the pool served none of does not read as a green pooled run.
        case NonFatal(thrown) =>
          println(
            s"[py-compile FALLBACK ${spawnFallbacks.incrementAndGet()}] $descriptorName: " +
              s"pooled worker unavailable, spawning instead: " +
              truncateBlock(thrown.toString, maxLines = 3, maxChars = 500)
          )
          pyCompile(pythonExecutable, pythonSource)
      }
    } else pyCompile(pythonExecutable, pythonSource)
  }

  /**
    * Runs `python -m py_compile` on the provided source, using an isolated interpreter invocation.
    * Retained as the pooled path's fallback and as the behavior selected by
    * TEXERA_TEST_PYTHON_WORKER=0.
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
    val ok = new AtomicInteger(0)
    val checked = new AtomicInteger(0)

    // Checked concurrently: the fan-out is what turns the pool's workers into
    // parallel interpreters rather than a queue in front of one. The executor is
    // sized to maxWorkers, so nothing is submitted past the cap.
    val allFindings = awaitAll(descriptorCandidates.map { descriptorClass => () =>
      val checkResult =
        PythonReflectionUtils.checkDescriptorWithCode(
          descriptorClass,
          rawInvalidText = RawInvalid,
          maxDepth = MaxDepth
        )
      checked.incrementAndGet()

      val pyCompileFindings = checkResult.code.toSeq.flatMap { generatedCode =>
        syntaxCheck(pythonExecutable, generatedCode, descriptorClass.getSimpleName) match {
          case Left(errorMessage) =>
            Seq(PythonReflectionUtils.Finding(descriptorClass.getName, "py-compile", errorMessage))
          case Right(()) => Nil
        }
      }

      val findings = checkResult.findings ++ pyCompileFindings

      if (findings.isEmpty && checkResult.code.nonEmpty) {
        println(
          s"[py-compile OK ${ok.incrementAndGet()}/$total | " +
            s"checked ${checked.get()}/$total] ${descriptorClass.getName}"
        )
      }

      findings
    }).flatten

    println(
      s"[py-compile SUMMARY] ok=${ok.get()}/$total, spawn fallbacks=${spawnFallbacks.get()}"
    )

    if (allFindings.nonEmpty) {
      fail(PythonReflectionUtils.renderReport(allFindings, total = total))
    }
  }

  /** py_compile above only parses the emitted code; running it needs the packages
    * it imports. Tagged, so only amber-integration — the job that installs them —
    * runs this. There a missing package is a defect; elsewhere it is a local-setup
    * fact, so cancel rather than fail.
    */
  test(
    "the Python interpreter operator templates run in should import pandas and plotly",
    Tag(classOf[IntegrationTest].getName)
  ) {
    val provisioned = sys.env.get("AMBER_TEST_FILTER").contains("integration-only")
    def unavailable(message: String): Nothing =
      if (provisioned) fail(message) else cancel(message)

    val python = loadPythonExeFromUdfConf().getOrElse(unavailable("no runnable python"))
    val imported = Try {
      val process = new ProcessBuilder(python, "-c", "import pandas, plotly")
        .redirectErrorStream(true)
        .start()
      // Killed on the way out: a probe that ran out of time is still running, and
      // would otherwise leak into the rest of the run.
      if (process.waitFor(60, TimeUnit.SECONDS)) process.exitValue() == 0
      else { process.destroyForcibly(); false }
    }
    if (!imported.getOrElse(false)) {
      unavailable(s"'$python' cannot import pandas and plotly")
    }
  }

}
