package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EpochMarkerHandler.PropagateEpochMarker
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ModifyLogicHandler.ModifyLogic
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ModifyOperatorLogicHandler.WorkerModifyLogicComplete
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.web.SubscriptionManager
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.request.ModifyLogicRequest
import edu.uci.ics.texera.web.model.websocket.response.{
  ModifyLogicCompletedEvent,
  ModifyLogicResponse
}
import edu.uci.ics.texera.web.storage.{JobReconfigurationStore, JobStateStore}
import edu.uci.ics.texera.workflow.common.workflow.WorkflowCompiler

import java.util.UUID
import scala.util.{Failure, Success}

class JobReconfigurationService(
    client: AmberClient,
    stateStore: JobStateStore,
    workflowCompiler: WorkflowCompiler,
    workflow: Workflow
) extends SubscriptionManager {

  // monitors notification from the engine that a reconfiguration on a worker is completed
  client.registerCallback[WorkerModifyLogicComplete]((evt: WorkerModifyLogicComplete) => {
    stateStore.reconfigurationStore.updateState(old => {
      old.copy(completedReconfigs = old.completedReconfigs + evt.workerID)
    })
  })

  // monitors the reconfiguration state (completed workers) change,
  // notifies the frontend when all workers of an operator complete reconfiguration
  addSubscription(
    stateStore.reconfigurationStore.registerDiffHandler((oldState, newState) => {
      if (
        oldState.completedReconfigs != newState.completedReconfigs
        && oldState.currentReconfigId == newState.currentReconfigId
      ) {
        val diff = newState.completedReconfigs -- oldState.completedReconfigs
        val newlyCompletedOps = diff
          .map(workerId => workflow.getOperator(workerId).id)
          .filter(opId =>
            workflow.getOperator(opId).getAllWorkers.toSet.subsetOf(newState.completedReconfigs)
          )
          .map(opId => opId.operator)
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
    val opId = newOp.operatorID
    workflowCompiler.initOperator(newOp)
    val currentOp = workflowCompiler.logicalPlan.operatorMap(opId)
    val reconfiguredPhysicalOp =
      currentOp.runtimeReconfiguration(newOp, workflowCompiler.logicalPlan.opSchemaInfo(opId))
    reconfiguredPhysicalOp match {
      case Failure(exception) => ModifyLogicResponse(opId, isValid = false, exception.getMessage)
      case Success(op) => {
        stateStore.reconfigurationStore.updateState(old =>
          old.copy(unscheduledReconfigs = old.unscheduledReconfigs :+ op)
        )
        ModifyLogicResponse(opId, isValid = true, "")
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
    val reconfigurations = stateStore.reconfigurationStore.getState.unscheduledReconfigs
    if (reconfigurations.isEmpty) {
      return
    }

    // schedule all pending reconfigurations to the engine
    val reconfigurationId = UUID.randomUUID().toString
    if (!Constants.enableTransactionalReconfiguration) {
      reconfigurations.foreach(reconfig => {
        client.sendAsync(ModifyLogic(reconfig._1, reconfig._2))
      })
    } else {
      val epochMarkers = FriesReconfigurationAlgorithm.scheduleReconfigurations(
        workflow.physicalPlan,
        reconfigurations,
        reconfigurationId
      )
      epochMarkers.foreach(epoch => {
        client.sendAsync(PropagateEpochMarker(epoch._1, epoch._2))
      })
    }

    // clear all un-scheduled reconfigurations, start a new reconfiguration ID
    stateStore.reconfigurationStore.updateState(old =>
      JobReconfigurationStore(Some(reconfigurationId))
    )
  }

}
