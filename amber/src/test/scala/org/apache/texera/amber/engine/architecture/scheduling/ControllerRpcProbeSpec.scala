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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.scheduling.RegionCoordinatorTestSupport._
import org.apache.texera.amber.engine.common.virtualidentity.util.CONTROLLER
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicReference

/**
  * Concurrency regression test for `ControllerRpcProbe`.
  *
  * The probe's `calls` buffer is appended to on a scheduler thread while the test thread reads it.
  * Before the fix, a read racing an append tripped Scala 2.13's `MutationTracker` and threw a
  * `ConcurrentModificationException`. This test forces that race so it fails deterministically
  * rather than as a CI flake.
  */
class ControllerRpcProbeSpec extends AnyFlatSpec {

  "ControllerRpcProbe" should "tolerate reads racing with concurrent appends" in {
    // Hold every endWorker pending so appends have no fulfill side effects.
    val probe = new ControllerRpcProbe(_ => None)
    val appends = 20000
    val failure = new AtomicReference[Throwable]()
    // Release both threads at once so the reader polls while the writer is still appending;
    // otherwise the writer could finish before the reader starts and miss the race entirely.
    val startGate = new CountDownLatch(1)

    // Writer: the actor side. Each sendTo drives handleOutput -> calls += call.
    val writer = new Thread(() => {
      try {
        startGate.await()
        var i = 0
        while (i < appends) {
          probe.outputGateway.sendTo(
            CONTROLLER,
            ControlInvocation(
              EndWorker,
              EmptyRequest(),
              AsyncRPCContext(CONTROLLER, CONTROLLER),
              i.toLong
            )
          )
          i += 1
        }
      } catch {
        case t: Throwable => failure.compareAndSet(null, t)
      }
    })

    // Reader: the test side. Read through every helper while appends are in flight.
    val reader = new Thread(() => {
      try {
        startGate.await()
        while (writer.isAlive) {
          probe.endWorkerCalls
          probe.methodTrace
          probe.initializedWorkers
          probe.startedWorkers
        }
      } catch {
        case t: Throwable => failure.compareAndSet(null, t)
      }
    })

    writer.start()
    reader.start()
    startGate.countDown()
    writer.join(testTimeout.inMilliseconds)
    reader.join(testTimeout.inMilliseconds)

    assert(!writer.isAlive && !reader.isAlive, "stress threads did not finish within the deadline")
    assert(
      failure.get() == null,
      s"concurrent access to the probe threw ${failure.get()}"
    )
    assert(probe.endWorkerCalls.size == appends)
  }
}
