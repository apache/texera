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

import com.fasterxml.jackson.databind.node.ObjectNode
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

  /** Short enough to keep the suite quick, far enough above process startup to
    * not be mistaken for one: a fixture asked to hang never answers at all, so
    * these deadlines cannot race it.
    */
  private val ShortTimeouts: PythonWorkerPool.Timeouts =
    PythonWorkerPool.Timeouts(responseMillis = 1500, startupMillis = 1500)

  /** For the case that interrupts its own callers: far enough out that they are
    * certainly still waiting on the worker, not already past their deadline.
    */
  private val PatientTimeouts: PythonWorkerPool.Timeouts =
    PythonWorkerPool.Timeouts(responseMillis = 60000, startupMillis = 60000)

  /** Ceiling on a whole case, well above the deadlines under test. Reaching it
    * means something never gave up.
    */
  private val Bound: FiniteDuration = 25.seconds

  /** Any interpreter serves — the fixture imports only `json` and `time` — so
    * this deliberately skips the configured `python.path` the suites that need
    * pandas resolve. A machine without one cancels rather than fails.
    */
  private lazy val python: String = {
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

  private def call(
      launchArgs: Seq[String],
      request: ObjectNode,
      timeouts: PythonWorkerPool.Timeouts
  ): PythonWorkerPool.Outcome =
    PythonWorkerPool.run(
      resourcePath = HangingWorker,
      launchArgs = launchArgs,
      pythonExe = python,
      request = request,
      interpreterArgs = Seq("-I", "-S"),
      timeouts = timeouts
    )

  /** A job the fixture takes and never answers. `hang` travels in the request
    * rather than in `launchArgs` so that [[healthyCall]] lands in the same
    * sub-pool: `Key` covers the script, its arguments and the environment, and a
    * job routed elsewhere would be served by a pool whose queue this suite never
    * touched.
    */
  private def hangingCall(
      launchArgs: Seq[String],
      request: ObjectNode = objectMapper.createObjectNode(),
      timeouts: PythonWorkerPool.Timeouts = ShortTimeouts
  ): PythonWorkerPool.Outcome =
    call(launchArgs, request.deepCopy().put("hang", true), timeouts)

  /** A job in that same sub-pool which the fixture does answer. */
  private def healthyCall(): PythonWorkerPool.Outcome =
    call(Seq.empty, objectMapper.createObjectNode(), ShortTimeouts)

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
    // One caller more than there are workers, all onto workers that go quiet:
    // those holding one time out and are discarded, which frees a slot without
    // handing anything back, and the caller waiting at the cap has to notice that
    // rather than wait for a hand-back that never comes. The discards it is
    // waiting on happen on workers other than the one ahead of it.
    val callers = PythonWorkerPool.maxWorkers + 1

    val outcomes = onDaemonThreads(callers) { implicit ec =>
      Await.result(Future.sequence(Seq.fill(callers)(Future(Try(hangingCall(Seq.empty))))), Bound)
    }

    assert(outcomes.length == callers)
    assert(outcomes.forall(_.isFailure))
  }

  test("the pool still serves jobs after it has discarded a timed-out worker") {
    interceptBounded(hangingCall(Seq.empty))

    // Deliberately the sub-pool the discard happened in — see [[hangingCall]] —
    // so what is asserted is that a pool short one worker starts a replacement,
    // not that an untouched pool works.
    assert(healthyCall().exit == 0)
  }

  test("an interpreter that cannot be started reaches the caller as a worker death") {
    // Not the IOException ProcessBuilder raises: a caller's fallback is written
    // against WorkerDiedException, and a pool that cannot hand out a worker at all
    // is the case that fallback exists for.
    val thrown = intercept[PythonWorkerPool.WorkerDiedException] {
      PythonWorkerPool.run(
        resourcePath = HangingWorker,
        launchArgs = Seq.empty,
        pythonExe = "no-such-python-on-this-machine",
        request = objectMapper.createObjectNode(),
        interpreterArgs = Seq("-I", "-S"),
        timeouts = ShortTimeouts
      )
    }

    assert(thrown.getMessage.contains("could not start python worker"))
  }

  test("a caller interrupted mid-job does not cost the pool a worker") {
    val workers = PythonWorkerPool.maxWorkers

    // Every slot taken by a job nobody will answer, and then the callers are
    // interrupted — `shutdownNow` is what an executor does to a fan-out whose test
    // has already failed. An interrupt is not a WorkerDiedException, so a worker
    // left neither returned nor discarded would cost this sub-pool that slot for
    // the rest of the JVM.
    onDaemonThreads(workers) { implicit ec =>
      Seq.fill(workers)(Future(Try(hangingCall(Seq.empty, timeouts = PatientTimeouts))))
      // Enough for the callers to be waiting on a worker rather than starting one;
      // the slot has to come back wherever the interrupt lands, so this only
      // decides which of the two paths the case exercises.
      Thread.sleep(500)
    }

    // Serving `workers` jobs again is the whole assertion: a leaked slot leaves
    // these at the cap, rechecking it until [[Bound]] runs out.
    val outcomes = onDaemonThreads(workers) { implicit ec =>
      Await.result(Future.sequence(Seq.fill(workers)(Future(healthyCall()))), Bound)
    }

    assert(outcomes.forall(_.exit == 0))
  }

  test("a worker whose first line is not the protocol is killed and reported") {
    val thrown = interceptBounded(hangingCall(Seq("--babble")))

    assert(thrown.getMessage.contains("did not signal ready"))
    // Named in the message: stderr is empty when a script writes its noise to
    // stdout, so the line itself is the only evidence of what went wrong.
    assert(thrown.getMessage.contains("not a protocol line"))
  }
}
