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

package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.NO_ALIGNMENT
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands._
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.TakeGlobalCheckpointResponse
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_PREPARE_CHECKPOINT
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF
import edu.uci.ics.amber.engine.common.{CheckpointState, SerializedState}
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

import java.net.URI

trait TakeGlobalCheckpointHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def takeGlobalCheckpoint(
      msg: TakeGlobalCheckpointRequest,
      ctx: AsyncRPCContext
  ): Future[TakeGlobalCheckpointResponse] = {
    var estimationOnly = msg.estimationOnly
    val destinationURI = new URI(msg.destination)
    @transient val storage =
      SequentialRecordStorage.getStorage[CheckpointState](Some(destinationURI))
    if (storage.containsFolder(msg.checkpointId.toString)) {
      logger.info("skip checkpoint since its already taken")
      estimationOnly = true
    }
    val uri = destinationURI.resolve(msg.checkpointId.toString)
    var totalSize = 0L
    val physicalOpIdsToTakeCheckpoint = cp.workflowScheduler.physicalPlan.operators.map(_.id)
    controllerInterface
      .propagateEmbeddedControlMessage(
        PropagateEmbeddedControlMessageRequest(
          cp.workflowExecution.getAllRegionExecutions
            .flatMap(_.getAllOperatorExecutions.map(_._1))
            .toSeq,
          msg.checkpointId,
          NO_ALIGNMENT,
          cp.workflowScheduler.physicalPlan.operators.map(_.id).toSeq,
          physicalOpIdsToTakeCheckpoint.toSeq,
          PrepareCheckpointRequest(msg.checkpointId, estimationOnly),
          METHOD_PREPARE_CHECKPOINT.getBareMethodName
        ),
        mkContext(SELF)
      )
      .flatMap { ret =>
        Future
          .collect(ret.returns.map {
            case (workerId, _) =>
              val destActor = ActorVirtualIdentity(workerId)
              workerInterface
                .finalizeCheckpoint(
                  FinalizeCheckpointRequest(msg.checkpointId, uri.toString),
                  mkContext(destActor)
                )
                .onSuccess { resp =>
                  totalSize += resp.size
                }
                .onFailure { err =>
                  throw err // TODO: handle failures.
                }
          }.toSeq)
          .map { _ =>
            logger.info("Start to take checkpoint")
            val chkpt = new CheckpointState()
            if (!estimationOnly) {
              // serialize CP state
              chkpt.save(SerializedState.CP_STATE_KEY, this.cp)
              logger.info(
                s"Serialized CP state, current workflow state = ${cp.workflowExecution.getState}"
              )
              // get all output messages from cp.transferService
              chkpt.save(
                SerializedState.OUTPUT_MSG_KEY,
                this.cp.transferService.getAllUnAckedMessages.toArray
              )
              val storage = SequentialRecordStorage.getStorage[CheckpointState](Some(uri))
              val writer = storage.getWriter(actorId.name)
              writer.writeRecord(chkpt)
              writer.flush()
              writer.close()
            }
            totalSize += chkpt.size()
            logger.info(s"global checkpoint finalized, total size = $totalSize")
            TakeGlobalCheckpointResponse(totalSize)
          }
      }
  }

}
