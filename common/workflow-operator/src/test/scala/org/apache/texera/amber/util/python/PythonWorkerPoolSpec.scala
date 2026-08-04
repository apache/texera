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

package org.apache.texera.amber.util.python

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeUnit
import scala.util.Try

/**
  * What the pool owes a caller when a worker misbehaves. An ordinary job failure
  * is the other suites' business; this one is about the two ways a worker stops
  * being usable, and it exists because only one of them ends by itself: a crash
  * closes the pipe and the read returns, while a worker that stays alive without
  * answering would hold the reading thread forever — a read on a process pipe
  * answers neither an interrupt nor a deadline, so a suite-level timeout cannot
  * unblock it.
  *
  * The fixture worker is stdlib-only and runs under `-I -S`, so this needs an
  * interpreter but none of the operator packages.
  */
final class PythonWorkerPoolSpec extends AnyFunSuite {

  private val HangingWorker = "/python/hanging_worker.py"
  private val CompileWorker = "/python/py_compile_worker.py"

  /** Short enough to keep the suite quick, far enough above process startup to
    * not be mistaken for one: the fixture never answers, so it cannot race.
    */
  private val ShortTimeouts: PythonWorkerPool.Timeouts =
    PythonWorkerPool.Timeouts(responseMillis = 1500, startupMillis = 1500)

  /** Any interpreter serves — the fixture imports only `json` and `time` — so
    * this deliberately skips the configured `python.path` the suites that need
    * pandas resolve. A machine without one cancels rather than fails.
    */
  private def python(): String = {
    def isRunnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (p.waitFor(5, TimeUnit.SECONDS)) p.exitValue() == 0 else { p.destroyForcibly(); false }
        }

    List("python3", "python", "py").find(isRunnable).getOrElse(cancel("no runnable python"))
  }

  private def runHanging(launchArgs: Seq[String]): PythonWorkerPool.WorkerDiedException =
    intercept[PythonWorkerPool.WorkerDiedException] {
      PythonWorkerPool.run(
        resourcePath = HangingWorker,
        launchArgs = launchArgs,
        pythonExe = python(),
        request = objectMapper.createObjectNode(),
        interpreterArgs = Seq("-I", "-S"),
        timeouts = ShortTimeouts
      )
    }

  test("a worker that takes the job and stops answering is killed and reported") {
    val startedAt = System.nanoTime()
    val thrown = runHanging(Seq.empty)
    val elapsedMillis = (System.nanoTime() - startedAt) / 1000000

    assert(thrown.getMessage.contains("did not answer"))
    assert(thrown.getMessage.contains("killed it"))
    // Well under the default response budget: what fired is the timeout passed in,
    // not a wait that happened to end.
    assert(elapsedMillis < PythonWorkerPool.Timeouts.Default.responseMillis / 2)
  }

  test("a worker that never signals ready is killed and reported") {
    val startedAt = System.nanoTime()
    val thrown = runHanging(Seq("--hang-before-ready"))
    val elapsedMillis = (System.nanoTime() - startedAt) / 1000000

    assert(thrown.getMessage.contains("did not signal ready"))
    assert(elapsedMillis < PythonWorkerPool.Timeouts.Default.startupMillis / 2)
  }

  test("the pool still serves jobs after it has discarded a timed-out worker") {
    runHanging(Seq.empty)

    val request = objectMapper.createObjectNode()
    request.put("source", "x = 1\n")
    request.put("name", "healthy.py")
    val outcome = PythonWorkerPool.run(
      resourcePath = CompileWorker,
      launchArgs = Seq.empty,
      pythonExe = python(),
      request = request,
      interpreterArgs = Seq("-I", "-S")
    )

    assert(outcome.exit == 0)
  }
}
