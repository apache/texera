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

import org.apache.texera.amber.core.executor.{
  OpExecInitInfo,
  OpExecWithClassName,
  OpExecWithCode,
  OperatorExecutor
}
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.InitializeExecutorRequest
import org.apache.texera.amber.engine.common.{CheckpointState, CheckpointSupport}
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalatest.flatspec.AnyFlatSpec

class SerializationManagerSpec extends AnyFlatSpec {

  // Build a real worker actor id via the same utility production uses, so
  // VirtualIdentityUtils.getWorkerIndex returns Some(idx) and the
  // "expected worker actor id" guard doesn't fire.
  private val workflowIdent = WorkflowIdentity(1L)
  private val opId = PhysicalOpIdentity(OperatorIdentity("op-a"), "main")
  private val workerActorId: ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(workflowIdent, opId, workerId = 0)
  // A non-worker actor id (created via the plain string constructor, not the
  // worker-identity factory) — VirtualIdentityUtils.getWorkerIndex will
  // return None for this, triggering the IllegalStateException guard.
  private val coordinatorActorId: ActorVirtualIdentity = ActorVirtualIdentity("coordinator")

  private def mkRequest(info: OpExecInitInfo, totalWorkers: Int = 1): InitializeExecutorRequest =
    InitializeExecutorRequest(
      totalWorkerCount = totalWorkers,
      opExecInitInfo = info,
      isSource = false,
      loopStartStateUris = Map.empty
    )

  "SerializationManager.restoreExecutorState" should
    "throw IllegalStateException when actorId is not a worker identity" in {
    val mgr = new SerializationManager(coordinatorActorId)
    mgr.setOpInitialization(
      mkRequest(
        OpExecWithClassName(
          className = classOf[SerializationManagerSpec.NoArgExec].getName,
          descString = ""
        )
      )
    )
    val ex = intercept[IllegalStateException] {
      mgr.restoreExecutorState(new CheckpointState())
    }
    assert(ex.getMessage.contains("worker"))
  }

  it should "instantiate via ExecFactory.newExecFromJavaClassName for OpExecWithClassName" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.setOpInitialization(
      mkRequest(
        OpExecWithClassName(
          className = classOf[SerializationManagerSpec.NoArgExec].getName,
          descString = ""
        )
      )
    )
    val (executor, iter) = mgr.restoreExecutorState(new CheckpointState())
    assert(executor.isInstanceOf[SerializationManagerSpec.NoArgExec])
    // Non-CheckpointSupport executor → empty restoration iterator.
    assert(iter.toList.isEmpty)
  }

  it should "throw UnsupportedOperationException on OpExecInitInfo.Empty (unsupported variant)" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.setOpInitialization(mkRequest(OpExecInitInfo.Empty))
    intercept[UnsupportedOperationException] {
      mgr.restoreExecutorState(new CheckpointState())
    }
  }

  it should "delegate to executor.deserializeState when the constructed executor is CheckpointSupport" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.setOpInitialization(
      mkRequest(
        OpExecWithClassName(
          className = classOf[SerializationManagerSpec.CheckpointAwareExec].getName,
          descString = ""
        )
      )
    )
    val (executor, iter) = mgr.restoreExecutorState(new CheckpointState())
    assert(executor.isInstanceOf[SerializationManagerSpec.CheckpointAwareExec])
    // The fixture returns a sentinel via deserializeState; if the
    // SerializationManager mistakenly used the non-CheckpointSupport
    // path (Iterator.empty), this would fail.
    val restored = iter.toList
    assert(restored.size == 1, s"expected one sentinel element, got: $restored")
  }

  it should "raise RuntimeException via the diagnostic path when OpExecWithCode is broken Java" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.setOpInitialization(
      mkRequest(OpExecWithCode(code = "public class JavaUDFOpExec { not valid }", language = ""))
    )
    val ex = intercept[RuntimeException] {
      mgr.restoreExecutorState(new CheckpointState())
    }
    assert(ex.getMessage.toLowerCase.contains("error"))
  }

  "SerializationManager.registerSerialization + applySerialization" should
    "invoke the registered callback exactly once and clear it afterward" in {
    val mgr = new SerializationManager(workerActorId)
    var calls = 0
    mgr.registerSerialization(() => calls += 1)
    mgr.applySerialization()
    assert(calls == 1)
    // A second applySerialization with no re-register must NOT re-invoke
    // the cleared callback (idempotency under the "fire once" contract).
    mgr.applySerialization()
    assert(calls == 1, "applySerialization must clear the callback after the first invocation")
  }

  it should "be a safe no-op when no callback has been registered" in {
    val mgr = new SerializationManager(workerActorId)
    mgr.applySerialization() // must not throw NPE
    succeed
  }

  it should "honor a re-registered callback after a previous applySerialization cleared it" in {
    val mgr = new SerializationManager(workerActorId)
    var first = 0
    var second = 0
    mgr.registerSerialization(() => first += 1)
    mgr.applySerialization()
    mgr.registerSerialization(() => second += 1)
    mgr.applySerialization()
    assert(first == 1)
    assert(second == 1)
  }
}

object SerializationManagerSpec {
  // No-arg executor fixture for the ExecFactory reflection path. Lives on
  // the companion (top-level binary name) so Class.forName + the no-arg
  // constructor reach it without an enclosing-instance reference.
  class NoArgExec extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }

  /** Mixes CheckpointSupport so the SerializationManager's
    * deserializeState branch is exercised. The fixture returns a single
    * sentinel element so the assertion can distinguish this branch from
    * the empty-iterator non-CheckpointSupport branch.
    */
  class CheckpointAwareExec extends OperatorExecutor with CheckpointSupport {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
    override def serializeState(
        currentIteratorState: Iterator[(TupleLike, Option[PortIdentity])],
        checkpoint: CheckpointState
    ): Iterator[(TupleLike, Option[PortIdentity])] = currentIteratorState
    override def deserializeState(
        checkpoint: CheckpointState
    ): Iterator[(TupleLike, Option[PortIdentity])] = {
      val sentinel: TupleLike =
        Tuple.builder(new org.apache.texera.amber.core.tuple.Schema()).build()
      Iterator((sentinel, None))
    }
    override def getEstimatedCheckpointCost: Long = 0L
  }
}
