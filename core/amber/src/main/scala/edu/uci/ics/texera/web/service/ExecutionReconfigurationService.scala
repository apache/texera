package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.web.SubscriptionManager
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.request.ModifyLogicRequest
import edu.uci.ics.texera.web.model.websocket.response.{
  ModifyLogicCompletedEvent,
  ModifyLogicResponse
}
import edu.uci.ics.texera.web.storage.ExecutionStateStore

import scala.util.{Failure, Success}

class ExecutionReconfigurationService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    workflow: Workflow
) extends SubscriptionManager {

  // monitors notification from the engine that a reconfiguration on a worker is completed
  //  client.registerCallback[UpdateExecutorCompleted]((evt: UpdateExecutorCompleted) => {
  //    stateStore.reconfigurationStore.updateState(old => {
  //      old.copy(completedReconfigurations = old.completedReconfigurations + evt.id)
  //    })
  //  })

  // monitors the reconfiguration state (completed workers) change,
  // notifies the frontend when all workers of an operator complete reconfiguration
  addSubscription(
    stateStore.reconfigurationStore.registerDiffHandler((oldState, newState) => {
      if (
        oldState.completedReconfigurations != newState.completedReconfigurations
        && oldState.currentReconfigId == newState.currentReconfigId
      ) {
        val diff = newState.completedReconfigurations -- oldState.completedReconfigurations
        val newlyCompletedOps = diff
          .map(workerId => workflow.physicalPlan.getPhysicalOpByWorkerId(workerId).id)
          .map(opId => opId.logicalOpId.id)
        if (newlyCompletedOps.nonEmpty) {
          List(ModifyLogicCompletedEvent(newlyCompletedOps.toList))
        } else {
          List()
        }
      } else {
        List()
      }
    })
  )

  // handles reconfigure workflow logic from frontend
  // validate the modify logic request and notifies the frontend
  // reconfigurations can only come when the workflow is paused,
  // they are not actually performed until the workflow is resumed
  def modifyOperatorLogic(modifyLogicRequest: ModifyLogicRequest): TexeraWebSocketEvent = {
    val newOp = modifyLogicRequest.operator
    val opId = newOp.operatorIdentifier
    val currentOp = workflow.logicalPlan.getOperator(opId)
    val reconfiguredPhysicalOp =
      currentOp.runtimeReconfiguration(
        workflow.context.workflowId,
        workflow.context.executionId,
        currentOp,
        newOp
      )
    reconfiguredPhysicalOp match {
      case Failure(exception) => ModifyLogicResponse(opId.id, isValid = false, exception.getMessage)
      case Success(op) => {
        stateStore.reconfigurationStore.updateState(old =>
          old.copy(unscheduledReconfigurations = old.unscheduledReconfigurations :+ op)
        )
        ModifyLogicResponse(opId.id, isValid = true, "")
      }
    }
  }

  // actually performs all reconfiguration requests the user made during pause
  // sends ModifyLogic messages to operators and workers,
  // there are two modes: transactional or non-transactional
  // in the transactional mode, reconfigurations on multiple operators will be synchronized
  // in the non-transaction mode, they are not synchronized, this is faster, but can lead to consistency issues
  // for details, see the Fries reconfiguration paper
  def performReconfigurationOnResume(): Unit = {
    val reconfigurations = stateStore.reconfigurationStore.getState.unscheduledReconfigurations
    if (reconfigurations.isEmpty) {
      return
    }
    throw new RuntimeException("reconfiguration is tentatively disabled.")
    //    // schedule all pending reconfigurations to the engine
    //    val reconfigurationId = UUID.randomUUID().toString
    //    val modifyLogicReq = AmberModifyLogicRequest(reconfigurations.map {
    //      case (op, stateTransferFunc) =>
    //        val bytes = AmberRuntime.serde.serialize(op.opExecInitInfo).get
    //        val protoAny = Any.of(
    //          "edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo",
    //          ByteString.copyFrom(bytes)
    //        )
    //        val stateTransferFuncOpt = stateTransferFunc.map { func =>
    //          val bytes = AmberRuntime.serde.serialize(func).get
    //          Any.of(
    //            "edu.uci.ics.texera.workflow.common.operators.StateTransferFunc",
    //            ByteString.copyFrom(bytes)
    //          )
    //        }
    //        UpdateExecutorRequest(op.id, protoAny, stateTransferFuncOpt)
    //    })
    //    client.controllerInterface.reconfigureWorkflow(
    //      WorkflowReconfigureRequest(modifyLogicReq, reconfigurationId),
    //      ()
    //    )
    //
    //    // clear all un-scheduled reconfigurations, start a new reconfiguration ID
    //    stateStore.reconfigurationStore.updateState(_ =>
    //      ExecutionReconfigurationStore(Some(reconfigurationId))
    //    )
  }
}
