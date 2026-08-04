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

import java.util.concurrent.{Executors, TimeUnit}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutorService, Future}
import scala.util.Try

/**
  * What the pool owes a caller when a worker misbehaves. An ordinary job failure
  * is the other suites' business; this one is about a worker that stays alive and
  * stops taking part, which is the case that does not end by itself: a crash
  * closes the pipe and the pending read returns, while silence would hold the
  * caller forever — neither a read nor a write on a process pipe answers an
  * interrupt or a deadline, so no suite-level timeout can release one.
  *
  * Every wait here is bounded and runs on daemon threads, so a regression fails
  * these tests instead of wedging the run: a lost non-daemon thread parked on a
  * pipe would keep the JVM, and the build, alive.
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

  /** Ceiling on a whole case, well above the deadlines under test. Reaching it
    * means something never gave up.
    */
  private val Bound: FiniteDuration = 25.seconds

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

  private def onDaemonThreads[T](threads: Int)(body: ExecutionContext => T): T = {
    val pool = Executors.newFixedThreadPool(
      threads,
      (r: Runnable) => {
        val t = new Thread(r, "pool-spec-caller")
        t.setDaemon(true)
        t
      }
    )
    val ec: ExecutionContextExecutorService = ExecutionContext.fromExecutorService(pool)
    try body(ec)
    finally pool.shutdownNow()
  }

  private def hangingCall(
      launchArgs: Seq[String],
      request: com.fasterxml.jackson.databind.node.ObjectNode = objectMapper.createObjectNode()
  ): PythonWorkerPool.Outcome =
    PythonWorkerPool.run(
      resourcePath = HangingWorker,
      launchArgs = launchArgs,
      pythonExe = python(),
      request = request,
      interpreterArgs = Seq("-I", "-S"),
      timeouts = ShortTimeouts
    )

  /** The call, on a daemon thread and under [[Bound]], expected to give up. */
  private def interceptBounded(call: => Any): PythonWorkerPool.WorkerDiedException =
    intercept[PythonWorkerPool.WorkerDiedException] {
      onDaemonThreads(1)(ec => Await.result(Future(call)(ec), Bound))
    }

  test("a worker that takes the job and stops answering is killed and reported") {
    val startedAt = System.nanoTime()
    val thrown = interceptBounded(hangingCall(Seq.empty))
    val elapsedMillis = (System.nanoTime() - startedAt) / 1000000

    assert(thrown.getMessage.contains("did not answer"))
    assert(thrown.getMessage.contains("killed it"))
    // Well under the default response budget: what fired is the timeout passed in,
    // not a wait that happened to end.
    assert(elapsedMillis < PythonWorkerPool.Timeouts.Default.responseMillis / 2)
  }

  test("a worker that never signals ready is killed and reported") {
    val startedAt = System.nanoTime()
    val thrown = interceptBounded(hangingCall(Seq("--hang-before-ready")))
    val elapsedMillis = (System.nanoTime() - startedAt) / 1000000

    assert(thrown.getMessage.contains("did not signal ready"))
    assert(elapsedMillis < PythonWorkerPool.Timeouts.Default.startupMillis / 2)
  }

  test("a worker that never reads its request is killed and reported") {
    val request = objectMapper.createObjectNode()
    // Past any pipe buffer, so the write cannot simply be handed to the kernel and
    // left there: it is the blocked write itself that has to be given up on.
    request.put("source", "x" * (4 * 1024 * 1024))

    val thrown = interceptBounded(hangingCall(Seq("--deaf"), request))

    assert(thrown.getMessage.contains("did not read its request"))
    assert(thrown.getMessage.contains("killed it"))
  }

  test("a caller waiting at the worker cap is not stranded by a discarded worker") {
    // One caller more than there are workers, all onto a worker that goes quiet:
    // those holding a worker time out and are discarded, which frees a slot
    // without handing anything back, and the caller waiting at the cap has to
    // notice that rather than wait for a hand-back that never comes.
    val callers = PythonWorkerPool.maxWorkers + 1

    val outcomes = onDaemonThreads(callers) { implicit ec =>
      Await.result(Future.sequence(Seq.fill(callers)(Future(Try(hangingCall(Seq.empty))))), Bound)
    }

    assert(outcomes.length == callers)
    assert(outcomes.forall(_.isFailure))
  }

  test("the pool still serves jobs after it has discarded a timed-out worker") {
    interceptBounded(hangingCall(Seq.empty))

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
