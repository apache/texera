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

package org.apache.texera.amber.engine.faulttolerance

import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.workflow.{PortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.worker.DataProcessor
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.DPInputQueueElement
import org.apache.texera.amber.engine.common.SerializedState.{CP_STATE_KEY, DP_STATE_KEY}
import org.apache.texera.amber.engine.common.virtualidentity.util.{COORDINATOR, SELF}
import org.apache.texera.amber.engine.common.{AmberRuntime, CheckpointState}
import org.apache.texera.amber.engine.e2e.TestUtils.buildWorkflow
import org.apache.texera.amber.operator.TestOperators
import org.apache.texera.common.compiler.model.LogicalLink
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.concurrent.LinkedBlockingQueue

class CheckpointSpec extends AnyFlatSpecLike with BeforeAndAfterAll {

  var system: ActorSystem = _

  val csvOpDesc = TestOperators.mediumCsvScanOpDesc()
  val keywordOpDesc = TestOperators.keywordSearchOpDesc("Region", "Asia")
  val workflow = buildWorkflow(
    List(csvOpDesc, keywordOpDesc),
    List(
      LogicalLink(
        csvOpDesc.operatorIdentifier,
        PortIdentity(),
        keywordOpDesc.operatorIdentifier,
        PortIdentity()
      )
    ),
    new WorkflowContext()
  )

  override def beforeAll(): Unit = {
    system = ActorSystem("CheckpointSpec", AmberRuntime.pekkoConfig)
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  "Default coordinator state" should "round-trip through CheckpointState" in {
    val cp =
      new CoordinatorProcessor(
        workflow.context,
        CoordinatorConfig.default,
        COORDINATOR,
        msg => {}
      )
    val chkpt = new CheckpointState()
    chkpt.save(CP_STATE_KEY, cp)
    val restored: CoordinatorProcessor = chkpt.load(CP_STATE_KEY)
    assert(restored.actorId == cp.actorId)
  }

  "Default worker state" should "round-trip through CheckpointState" in {
    val dp = new DataProcessor(
      SELF,
      msg => {},
      inputMessageQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    )
    val chkpt = new CheckpointState()
    chkpt.save(DP_STATE_KEY, dp)
    val restored: DataProcessor = chkpt.load(DP_STATE_KEY)
    assert(restored.actorId == dp.actorId)
  }

  "CheckpointState" should "fail loudly on an unknown key" in {
    // Pin the documented contract precisely: load throws
    // RuntimeException("no state saved for key = $key"). A bare
    // `contains("unknown")` would still pass if the message ever drifts to
    // something like "unknown checkpoint", silently weakening the assertion.
    val chkpt = new CheckpointState()
    assert(!chkpt.has("unknown"))
    val ex = intercept[RuntimeException] {
      chkpt.load[Any]("unknown")
    }
    assert(ex.getMessage == "no state saved for key = unknown")
  }

  // Checkpoint coverage beyond these round-trips lives elsewhere: SerializationManagerSpec and
  // CheckpointSubsystemSpec cover operator and DP state going through a CheckpointState, and
  // PrepareCheckpointHandlerSpec / FinalizeCheckpointHandlerSpec /
  // TakeGlobalCheckpointHandlerSpec cover the promise handlers that drive a checkpoint. A full
  // "checkpoint, reload, continue" run needs a live multi-operator workflow and belongs in an
  // integration spec, not here.

}
