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

package org.apache.texera.amber.engine.architecture.deploysemantics.layer

import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.worker.statistics.{WorkerState, WorkerStatistics}
import org.scalatest.flatspec.AnyFlatSpec

class WorkerExecutionSpec extends AnyFlatSpec {

  private def stats(idle: Long): WorkerStatistics =
    WorkerStatistics(Seq.empty, Seq.empty, 0L, 0L, idle)

  "WorkerExecution" should "have UNINITIALIZED state and zeroed stats by default" in {
    val we = WorkerExecution()
    assert(we.getState == WorkerState.UNINITIALIZED)
    assert(we.getStats.idleTime == 0L)
    assert(we.getStats.dataProcessingTime == 0L)
    assert(we.getStats.controlProcessingTime == 0L)
  }

  // ---- state ordering by logical version ----

  "WorkerExecution.updateState" should "apply a state with a newer version" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 1L, WorkerState.READY)
    we.updateState(stateVersion = 2L, WorkerState.RUNNING)
    assert(we.getState == WorkerState.RUNNING)
  }

  it should "apply the very first report (version 0)" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 0L, WorkerState.READY)
    assert(we.getState == WorkerState.READY)
  }

  it should "ignore a stale report whose version is lower (out-of-order arrival)" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 5L, WorkerState.PAUSED)
    we.updateState(stateVersion = 4L, WorkerState.RUNNING) // arrives late, older version
    assert(we.getState == WorkerState.PAUSED)
  }

  it should "ignore a report whose version is not strictly newer" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 3L, WorkerState.RUNNING)
    we.updateState(stateVersion = 3L, WorkerState.PAUSED) // same version
    assert(we.getState == WorkerState.RUNNING)
  }

  // ---- terminal-state absorption ----

  it should "treat COMPLETED as absorbing even against a higher-version report" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 2L, WorkerState.COMPLETED)
    we.updateState(stateVersion = 99L, WorkerState.RUNNING) // higher version, but illegal
    assert(we.getState == WorkerState.COMPLETED)
  }

  it should "treat TERMINATED as absorbing" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 2L, WorkerState.TERMINATED)
    we.updateState(stateVersion = 99L, WorkerState.RUNNING)
    assert(we.getState == WorkerState.TERMINATED)
  }

  // Regression for issue #6010: a fast source's startWorker response carries a stale
  // RUNNING snapshot that can reach the controller AFTER COMPLETED was recorded. With
  // wall-clock ordering the late RUNNING won and the operator was stuck orange; with
  // version ordering (and terminal absorption) COMPLETED must survive.
  it should "not let a late startWorker RUNNING snapshot clobber COMPLETED (#6010)" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 1L, WorkerState.READY)
    we.updateState(stateVersion = 3L, WorkerState.COMPLETED) // via completion stats query
    we.updateState(stateVersion = 2L, WorkerState.RUNNING) // late startWorker response
    assert(we.getState == WorkerState.COMPLETED)
  }

  // ---- forceTerminate ----

  "WorkerExecution.forceTerminate" should "move a non-terminal worker to TERMINATED" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 2L, WorkerState.RUNNING)
    we.forceTerminate()
    assert(we.getState == WorkerState.TERMINATED)
  }

  it should "leave a worker that already COMPLETED as COMPLETED" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 3L, WorkerState.COMPLETED)
    we.forceTerminate()
    assert(we.getState == WorkerState.COMPLETED)
  }

  // ---- stats ordering by timestamp ----

  "WorkerExecution.updateStats" should "apply newer stats and keep state untouched" in {
    val we = WorkerExecution()
    we.updateState(stateVersion = 2L, WorkerState.RUNNING)
    we.updateStats(timeStamp = 10L, stats(idle = 7L))
    we.updateStats(timeStamp = 20L, stats(idle = 42L))
    assert(we.getState == WorkerState.RUNNING)
    assert(we.getStats.idleTime == 42L)
  }

  it should "ignore stats with a non-newer timestamp" in {
    val we = WorkerExecution()
    we.updateStats(timeStamp = 20L, stats(idle = 42L))
    we.updateStats(timeStamp = 20L, stats(idle = 99L)) // not strictly newer
    we.updateStats(timeStamp = 5L, stats(idle = 0L)) // older
    assert(we.getStats.idleTime == 42L)
  }

  it should "track state version and stats timestamp independently" in {
    val we = WorkerExecution()
    // A high stats timestamp must not block a later (higher-version) state update,
    // and vice versa: the two orderings are independent.
    we.updateStats(timeStamp = 1000L, stats(idle = 1L))
    we.updateState(stateVersion = 1L, WorkerState.RUNNING)
    assert(we.getState == WorkerState.RUNNING)
    assert(we.getStats.idleTime == 1L)
  }

  // ---- port executions ----

  "WorkerExecution.getInputPortExecution" should "lazily create and reuse a port execution per port id" in {
    val we = WorkerExecution()
    val first = we.getInputPortExecution(PortIdentity(0))
    val same = we.getInputPortExecution(PortIdentity(0))
    val other = we.getInputPortExecution(PortIdentity(1))
    assert(first eq same)
    assert(first ne other)
  }

  "WorkerExecution.getOutputPortExecution" should "lazily create and reuse a port execution per port id" in {
    val we = WorkerExecution()
    val first = we.getOutputPortExecution(PortIdentity(0))
    val same = we.getOutputPortExecution(PortIdentity(0))
    assert(first eq same)
  }

  it should "use a separate map from getInputPortExecution" in {
    val we = WorkerExecution()
    val input = we.getInputPortExecution(PortIdentity(0))
    val output = we.getOutputPortExecution(PortIdentity(0))
    assert(input ne output)
  }
}
