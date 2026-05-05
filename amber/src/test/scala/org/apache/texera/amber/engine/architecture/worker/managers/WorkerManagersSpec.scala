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

package org.apache.texera.amber.engine.architecture.worker.managers

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.scalatest.flatspec.AnyFlatSpec

class WorkerManagersSpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // StatisticsManager
  // ---------------------------------------------------------------------------

  // Minimal OperatorExecutor instance — StatisticsManager.getStatistics ignores
  // its argument today, so any concrete impl works.
  private val nullExec: OperatorExecutor = new OperatorExecutor {
    override def processTuple(t: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }

  "StatisticsManager" should "default all counters to zero" in {
    val sm = new StatisticsManager()
    assert(sm.getInputTupleCount == 0L)
    assert(sm.getOutputTupleCount == 0L)
    val s = sm.getStatistics(nullExec)
    assert(s.inputTupleMetrics.isEmpty)
    assert(s.outputTupleMetrics.isEmpty)
    assert(s.dataProcessingTime == 0L)
    assert(s.controlProcessingTime == 0L)
    // totalExecutionTime - data - control = 0 - 0 - 0 = 0
    assert(s.idleTime == 0L)
  }

  "StatisticsManager.increaseInputStatistics" should "accumulate count and size per port" in {
    val sm = new StatisticsManager()
    sm.increaseInputStatistics(PortIdentity(0), 100)
    sm.increaseInputStatistics(PortIdentity(0), 50)
    sm.increaseInputStatistics(PortIdentity(1), 25)
    assert(sm.getInputTupleCount == 3L)
    val byPort = sm
      .getStatistics(nullExec)
      .inputTupleMetrics
      .map(m => m.portId -> (m.tupleMetrics.count, m.tupleMetrics.size))
      .toMap
    assert(byPort(PortIdentity(0)) == (2L, 150L))
    assert(byPort(PortIdentity(1)) == (1L, 25L))
  }

  it should "reject negative tuple sizes" in {
    val sm = new StatisticsManager()
    assertThrows[IllegalArgumentException] {
      sm.increaseInputStatistics(PortIdentity(0), -1)
    }
  }

  "StatisticsManager.increaseOutputStatistics" should "accumulate count and size per port" in {
    val sm = new StatisticsManager()
    sm.increaseOutputStatistics(PortIdentity(0), 30)
    sm.increaseOutputStatistics(PortIdentity(0), 70)
    assert(sm.getOutputTupleCount == 2L)
    val out = sm.getStatistics(nullExec).outputTupleMetrics
    assert(out.size == 1)
    assert(out.head.tupleMetrics.count == 2L)
    assert(out.head.tupleMetrics.size == 100L)
  }

  it should "reject negative tuple sizes" in {
    val sm = new StatisticsManager()
    assertThrows[IllegalArgumentException] {
      sm.increaseOutputStatistics(PortIdentity(0), -1)
    }
  }

  "StatisticsManager.increaseDataProcessingTime" should "accumulate time and reject negatives" in {
    val sm = new StatisticsManager()
    sm.increaseDataProcessingTime(100)
    sm.increaseDataProcessingTime(50)
    assert(sm.getStatistics(nullExec).dataProcessingTime == 150L)
    assertThrows[IllegalArgumentException] {
      sm.increaseDataProcessingTime(-1)
    }
  }

  "StatisticsManager.increaseControlProcessingTime" should "accumulate time and reject negatives" in {
    val sm = new StatisticsManager()
    sm.increaseControlProcessingTime(20)
    sm.increaseControlProcessingTime(40)
    assert(sm.getStatistics(nullExec).controlProcessingTime == 60L)
    assertThrows[IllegalArgumentException] {
      sm.increaseControlProcessingTime(-1)
    }
  }

  "StatisticsManager.updateTotalExecutionTime" should "compute idleTime as total - data - control" in {
    val sm = new StatisticsManager()
    sm.initializeWorkerStartTime(1000L)
    sm.increaseDataProcessingTime(200L)
    sm.increaseControlProcessingTime(100L)
    sm.updateTotalExecutionTime(2000L)
    val s = sm.getStatistics(nullExec)
    assert(s.dataProcessingTime == 200L)
    assert(s.controlProcessingTime == 100L)
    assert(s.idleTime == 2000L - 1000L - 200L - 100L)
  }

  it should "reject a current time before workerStartTime" in {
    val sm = new StatisticsManager()
    sm.initializeWorkerStartTime(1000L)
    assertThrows[IllegalArgumentException] {
      sm.updateTotalExecutionTime(500L)
    }
  }

  // ---------------------------------------------------------------------------
  // SerializationManager
  // ---------------------------------------------------------------------------

  "SerializationManager.applySerialization" should "be a no-op when no callback is registered" in {
    val sm = new SerializationManager(ActorVirtualIdentity("worker-1"))
    sm.applySerialization() // does not throw
    succeed
  }

  it should "invoke the registered callback exactly once and then clear it" in {
    val sm = new SerializationManager(ActorVirtualIdentity("worker-1"))
    var calls = 0
    sm.registerSerialization(() => calls += 1)
    sm.applySerialization()
    sm.applySerialization() // second call must be a no-op (callback was cleared)
    assert(calls == 1)
  }

  it should "let the latest registered callback overwrite any previous one" in {
    val sm = new SerializationManager(ActorVirtualIdentity("worker-1"))
    var firstCalls = 0
    var secondCalls = 0
    sm.registerSerialization(() => firstCalls += 1)
    sm.registerSerialization(() => secondCalls += 1)
    sm.applySerialization()
    assert(firstCalls == 0)
    assert(secondCalls == 1)
  }

  // ---------------------------------------------------------------------------
  // PauseManager (with a stub InputGateway)
  // ---------------------------------------------------------------------------

  import org.apache.texera.amber.engine.architecture.logreplay.OrderEnforcer
  import org.apache.texera.amber.engine.architecture.messaginglayer.{AmberFIFOChannel, InputGateway}
  import org.apache.texera.amber.engine.architecture.worker.{
    BackpressurePause,
    OperatorLogicPause,
    PauseManager,
    UserPause
  }

  /**
    * Stub gateway with a fixed set of channels. `tryPickChannel` /
    * `tryPickControlChannel` are unused by PauseManager and return None.
    */
  private class StubGateway(channels: Map[ChannelIdentity, AmberFIFOChannel]) extends InputGateway {
    override def tryPickControlChannel: Option[AmberFIFOChannel] = None
    override def tryPickChannel: Option[AmberFIFOChannel] = None
    override def getAllChannels: Iterable[AmberFIFOChannel] = channels.values
    override def getAllDataChannels: Iterable[AmberFIFOChannel] =
      channels.collect { case (cid, ch) if !cid.isControl => ch }
    override def getChannel(channelId: ChannelIdentity): AmberFIFOChannel = channels(channelId)
    override def getAllControlChannels: Iterable[AmberFIFOChannel] =
      channels.collect { case (cid, ch) if cid.isControl => ch }
    override def addEnforcer(enforcer: OrderEnforcer): Unit = ()
  }

  private val workerId = ActorVirtualIdentity("w")
  private val dataA =
    ChannelIdentity(ActorVirtualIdentity("up1"), workerId, isControl = false)
  private val dataB =
    ChannelIdentity(ActorVirtualIdentity("up2"), workerId, isControl = false)
  private val ctrl =
    ChannelIdentity(ActorVirtualIdentity("ctrl"), workerId, isControl = true)

  private def newGateway(): (StubGateway, AmberFIFOChannel, AmberFIFOChannel, AmberFIFOChannel) = {
    val a = new AmberFIFOChannel(dataA)
    val b = new AmberFIFOChannel(dataB)
    val c = new AmberFIFOChannel(ctrl)
    val gw = new StubGateway(Map(dataA -> a, dataB -> b, ctrl -> c))
    (gw, a, b, c)
  }

  "PauseManager.isPaused" should "be false initially" in {
    val (gw, _, _, _) = newGateway()
    val pm = new PauseManager(workerId, gw)
    assert(!pm.isPaused)
  }

  "PauseManager.pause" should "disable every data channel and report paused" in {
    val (gw, a, b, c) = newGateway()
    val pm = new PauseManager(workerId, gw)
    pm.pause(UserPause)
    assert(pm.isPaused)
    assert(!a.isEnabled)
    assert(!b.isEnabled)
    // control channel is not in getAllDataChannels, so it stays enabled
    assert(c.isEnabled)
  }

  "PauseManager.resume" should "re-enable all data channels when no specific input pauses remain" in {
    val (gw, a, b, c) = newGateway()
    val pm = new PauseManager(workerId, gw)
    pm.pause(UserPause)
    pm.resume(UserPause)
    assert(!pm.isPaused)
    assert(a.isEnabled)
    assert(b.isEnabled)
    assert(c.isEnabled)
  }

  it should "stay paused if other global pauses are still active" in {
    val (gw, a, _, _) = newGateway()
    val pm = new PauseManager(workerId, gw)
    pm.pause(UserPause)
    pm.pause(BackpressurePause)
    pm.resume(UserPause)
    // backpressure still pausing → channels stay disabled
    assert(pm.isPaused)
    assert(!a.isEnabled)
  }

  "PauseManager.pauseInputChannel" should "disable only the listed channels" in {
    val (gw, a, b, _) = newGateway()
    val pm = new PauseManager(workerId, gw)
    pm.pauseInputChannel(OperatorLogicPause, List(dataA))
    // global pauses are empty → not "isPaused"
    assert(!pm.isPaused)
    assert(!a.isEnabled)
    assert(b.isEnabled)
  }

  it should "leave still-paused specific channels disabled when only one of multiple specific pauses is resumed" in {
    val (gw, a, b, _) = newGateway()
    val pm = new PauseManager(workerId, gw)
    pm.pauseInputChannel(OperatorLogicPause, List(dataA))
    pm.pauseInputChannel(BackpressurePause, List(dataB))
    pm.resume(OperatorLogicPause)
    // dataA's only specific pause was OperatorLogicPause → re-enabled.
    // dataB still has BackpressurePause → still disabled.
    assert(a.isEnabled)
    assert(!b.isEnabled)
  }
}
