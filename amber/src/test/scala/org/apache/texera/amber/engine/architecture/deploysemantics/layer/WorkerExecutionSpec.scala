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

  "WorkerExecution.update(state)" should "apply when the timestamp is newer" in {
    val we = WorkerExecution()
    we.update(timeStamp = 10L, state = WorkerState.RUNNING)
    assert(we.getState == WorkerState.RUNNING)
  }

  it should "ignore updates with a non-newer timestamp" in {
    val we = WorkerExecution()
    we.update(timeStamp = 10L, state = WorkerState.RUNNING)
    we.update(timeStamp = 10L, state = WorkerState.PAUSED) // not strictly newer
    we.update(timeStamp = 5L, state = WorkerState.COMPLETED) // older
    assert(we.getState == WorkerState.RUNNING)
  }

  "WorkerExecution.update(state, stats)" should "update both atomically when newer" in {
    val we = WorkerExecution()
    we.update(timeStamp = 10L, state = WorkerState.RUNNING, stats = stats(idle = 7L))
    assert(we.getState == WorkerState.RUNNING)
    assert(we.getStats.idleTime == 7L)
  }

  it should "ignore updates with a non-newer timestamp" in {
    val we = WorkerExecution()
    we.update(timeStamp = 10L, state = WorkerState.RUNNING, stats = stats(idle = 7L))
    we.update(timeStamp = 5L, state = WorkerState.COMPLETED, stats = stats(idle = 99L))
    assert(we.getState == WorkerState.RUNNING)
    assert(we.getStats.idleTime == 7L)
  }

  "WorkerExecution.update(stats)" should "update only the stats when newer" in {
    val we = WorkerExecution()
    we.update(timeStamp = 10L, state = WorkerState.RUNNING, stats = stats(idle = 7L))
    we.update(timeStamp = 20L, stats = stats(idle = 42L))
    assert(we.getState == WorkerState.RUNNING)
    assert(we.getStats.idleTime == 42L)
  }

  it should "ignore stats updates with a non-newer timestamp" in {
    val we = WorkerExecution()
    we.update(timeStamp = 20L, stats = stats(idle = 42L))
    we.update(timeStamp = 20L, stats = stats(idle = 99L)) // not strictly newer
    we.update(timeStamp = 5L, stats = stats(idle = 0L)) // older
    assert(we.getStats.idleTime == 42L)
  }

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
