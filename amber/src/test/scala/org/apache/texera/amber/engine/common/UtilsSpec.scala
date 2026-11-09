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

package org.apache.texera.amber.engine.common

import com.twitter.util.{Await, Future, Time}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.file.Paths
import java.util.concurrent.locks.ReentrantLock
import scala.collection.mutable

class UtilsSpec extends AnyFlatSpec {

  // -- amberHomePath --------------------------------------------------------

  "Utils.AMBER_HOME_FOLDER_NAME" should "be the literal directory name `amber`" in {
    // Pinned on its own so that the search result below can be compared against a
    // literal rather than against the constant that selected it -- comparing the two
    // would move both sides of the equality together and detect nothing.
    assert(Utils.AMBER_HOME_FOLDER_NAME == "amber")
  }

  "Utils.amberHomePath" should "resolve to an `amber` directory beneath the working directory" in {
    // The tests run from the repo root, which is not itself amber home, so this
    // exercises the Files.walk search rather than the early return.
    //
    // Only the *file name* is asserted, never the whole path: `findAny` on a
    // sequential walk returns the first pre-order directory whose name ends in
    // "amber", and dot-prefixed sibling directories are visited before `amber/`
    // itself -- so a local `.<tool>/amber` legitimately wins here (the same hazard
    // ArrowFlightActorBench documents).
    //
    // NOT covered here: the `2` in `Files.walk(cwd, 2)`. `amberHomePath` is a lazy
    // val reading the process working directory, and `amber/` is a direct child of
    // the repo root, so depth 1 finds it just as well and any depth >= 1 passes every
    // assertion below. Only depth 0 dies. Pinning the documented grandchild case
    // would need an injectable start directory in production code.
    val currentWorkingDirectory = Paths.get(".").toRealPath()
    val amberHome = Utils.amberHomePath

    // The literal, never `Utils.AMBER_HOME_FOLDER_NAME`: the constant is what decides
    // which directory the walk accepts, so asserting against it is a tautology.
    assert(amberHome.getFileName.toString == "amber")
    // Pinning it as a strict descendant is what keeps the assertion above from
    // passing vacuously: on the early-return branch the result IS the working
    // directory, so a run that never reached the walk would fail here.
    assert(amberHome.startsWith(currentWorkingDirectory))
    assert(amberHome != currentWorkingDirectory)
  }

  // -- aggregatedStateToString ----------------------------------------------

  "Utils.aggregatedStateToString" should "round-trip every named WorkflowAggregatedState through stringToAggregatedState" in {
    val namedStates = Seq(
      WorkflowAggregatedState.UNINITIALIZED,
      WorkflowAggregatedState.READY,
      WorkflowAggregatedState.RUNNING,
      WorkflowAggregatedState.PAUSING,
      WorkflowAggregatedState.PAUSED,
      WorkflowAggregatedState.RESUMING,
      WorkflowAggregatedState.COMPLETED,
      WorkflowAggregatedState.TERMINATED,
      WorkflowAggregatedState.FAILED,
      WorkflowAggregatedState.KILLED,
      WorkflowAggregatedState.UNKNOWN
    )
    namedStates.foreach { state =>
      assert(
        Utils.stringToAggregatedState(Utils.aggregatedStateToString(state)) == state,
        s"round-trip failed for $state"
      )
    }
  }

  it should "render an unrecognized aggregated state with its raw value" in {
    val unrecognized = WorkflowAggregatedState.Unrecognized(99)
    assert(Utils.aggregatedStateToString(unrecognized) == "Unrecognized(99)")
  }

  // -- stringToAggregatedState ----------------------------------------------

  "Utils.stringToAggregatedState" should "be case-insensitive and tolerant of surrounding whitespace" in {
    assert(Utils.stringToAggregatedState("RUNNING") == WorkflowAggregatedState.RUNNING)
    assert(Utils.stringToAggregatedState("running") == WorkflowAggregatedState.RUNNING)
    assert(Utils.stringToAggregatedState("  Running  ") == WorkflowAggregatedState.RUNNING)
  }

  it should "accept 'Initializing' as an alias for READY" in {
    assert(Utils.stringToAggregatedState("Initializing") == WorkflowAggregatedState.READY)
    assert(Utils.stringToAggregatedState("ready") == WorkflowAggregatedState.READY)
  }

  it should "throw IllegalArgumentException for an unrecognized state name" in {
    assertThrows[IllegalArgumentException] {
      Utils.stringToAggregatedState("not-a-real-state")
    }
  }

  // -- maptoStatusCode ------------------------------------------------------

  "Utils.maptoStatusCode" should "map known states to their documented byte codes" in {
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.UNINITIALIZED) == 0.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.READY) == 0.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.RUNNING) == 1.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.PAUSED) == 2.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.COMPLETED) == 3.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.FAILED) == 4.toByte)
    assert(Utils.maptoStatusCode(WorkflowAggregatedState.KILLED) == 5.toByte)
  }

  it should "return -1 for states that have no documented code" in {
    Seq(
      WorkflowAggregatedState.PAUSING,
      WorkflowAggregatedState.RESUMING,
      WorkflowAggregatedState.TERMINATED,
      WorkflowAggregatedState.UNKNOWN
    ).foreach { state =>
      assert(Utils.maptoStatusCode(state) == -1.toByte, s"expected -1 for $state")
    }
  }

  // -- retry ---------------------------------------------------------------

  // `retry` is exercised with `RecordingInlineTimer`: it captures the delay of each scheduled wait
  // and runs it immediately, so the backoff schedule is asserted exactly and no test spends that
  // time asleep. `Time.withCurrentTimeFrozen` makes `when - Time.now` equal the delay
  // `Future.sleep` asked for.

  "Utils.retry" should "return the value on the first successful attempt without waiting" in {
    val timer = new RecordingInlineTimer
    var calls = 0
    val result = Await.result(
      Utils.retry(attempts = 3, baseBackoffTimeInMS = 200L, timer = timer) {
        calls += 1
        Future.value("ok")
      }
    )
    assert(result == "ok")
    assert(calls == 1)
    assert(timer.recordedDelaysInMillis.isEmpty)
  }

  it should "double the backoff after each failed attempt" in {
    val timer = new RecordingInlineTimer
    var calls = 0
    Time.withCurrentTimeFrozen { _ =>
      val failure = intercept[RuntimeException] {
        Await.result(
          Utils.retry(attempts = 4, baseBackoffTimeInMS = 200L, timer = timer) {
            calls += 1
            Future.exception(new RuntimeException(s"failure-$calls"))
          }
        )
      }
      // The last failure is what surfaces, and 4 attempts spend exactly 3 waits.
      assert(failure.getMessage == "failure-4")
      assert(calls == 4)
      assert(timer.recordedDelaysInMillis == Seq(200L, 400L, 800L))
    }
  }

  it should "stop waiting as soon as an attempt succeeds" in {
    val timer = new RecordingInlineTimer
    var calls = 0
    Time.withCurrentTimeFrozen { _ =>
      val result = Await.result(
        Utils.retry(attempts = 4, baseBackoffTimeInMS = 200L, timer = timer) {
          calls += 1
          if (calls < 3) Future.exception(new RuntimeException("transient"))
          else Future.value(calls)
        }
      )
      assert(result == 3)
      assert(timer.recordedDelaysInMillis == Seq(200L, 400L))
    }
  }

  it should "report the failed attempt number and the upcoming backoff to onRetry" in {
    val timer = new RecordingInlineTimer
    val observed = mutable.ArrayBuffer[(String, Int, Long)]()
    Time.withCurrentTimeFrozen { _ =>
      intercept[RuntimeException] {
        Await.result(
          Utils.retry(
            attempts = 3,
            baseBackoffTimeInMS = 200L,
            timer = timer,
            onRetry =
              (err, attempt, backoffMs) => observed += ((err.getMessage, attempt, backoffMs))
          ) {
            Future.exception(new RuntimeException("boom"))
          }
        )
      }
      // Called once per wait (never after the final attempt), with 1-based attempt numbers.
      assert(observed.toSeq == Seq(("boom", 1, 200L), ("boom", 2, 400L)))
    }
  }

  it should "retry a synchronous throw from the body, not just a failed Future" in {
    // The body is by-name, so a `Future`-returning expression can still blow up before it ever
    // produces a Future; that must be retried like any other failure rather than escaping.
    val timer = new RecordingInlineTimer
    var calls = 0
    val result = Await.result(
      Utils.retry(attempts = 3, baseBackoffTimeInMS = 1L, timer = timer) {
        calls += 1
        if (calls < 2) throw new IllegalStateException("sync boom")
        Future.value("ok")
      }
    )
    assert(result == "ok")
    assert(calls == 2)
  }

  it should "not retry a fatal error, whether thrown or returned as a failed Future" in {
    // A fatal is not a transient failure, so it must escape on the first attempt. The two shapes
    // travel different paths -- a synchronous throw is filtered by `Future(fn)`'s `Try`, a failed
    // `Future` reaches the `rescue` guard -- and both have to behave the same way.
    Seq[(String, () => Future[String])](
      "as a failed Future" -> (() => Future.exception(new InterruptedException("fatal"))),
      "thrown synchronously" -> (() => throw new InterruptedException("fatal"))
    ).foreach {
      case (shape, body) =>
        val timer = new RecordingInlineTimer
        var calls = 0
        intercept[InterruptedException] {
          Await.result(
            Utils.retry(attempts = 4, baseBackoffTimeInMS = 200L, timer = timer) {
              calls += 1
              body()
            }
          )
        }
        assert(calls == 1, s"fatal $shape was retried")
        assert(timer.recordedDelaysInMillis.isEmpty, s"fatal $shape caused a backoff wait")
    }
  }

  it should "make a single attempt when attempts is one or less" in {
    // A budget of 1 -- or a nonsensical 0 -- means no retry at all, and no wait either.
    Seq(1, 0).foreach { attempts =>
      val timer = new RecordingInlineTimer
      var calls = 0
      val failure = intercept[RuntimeException] {
        Await.result(
          Utils.retry(attempts = attempts, baseBackoffTimeInMS = 200L, timer = timer) {
            calls += 1
            Future.exception(new RuntimeException("only-once"))
          }
        )
      }
      assert(failure.getMessage == "only-once", s"attempts = $attempts")
      assert(calls == 1, s"attempts = $attempts")
      assert(timer.recordedDelaysInMillis.isEmpty, s"attempts = $attempts")
    }
  }

  // -- withLock ------------------------------------------------------------

  "Utils.withLock" should "release the lock after the body returns" in {
    implicit val lock: ReentrantLock = new ReentrantLock()
    val result = Utils.withLock {
      assert(lock.isHeldByCurrentThread)
      42
    }
    assert(result == 42)
    assert(!lock.isHeldByCurrentThread)
  }

  it should "release the lock when the body throws" in {
    implicit val lock: ReentrantLock = new ReentrantLock()
    intercept[RuntimeException] {
      Utils.withLock[Unit] {
        throw new RuntimeException("boom")
      }
    }
    assert(!lock.isHeldByCurrentThread)
  }
}
