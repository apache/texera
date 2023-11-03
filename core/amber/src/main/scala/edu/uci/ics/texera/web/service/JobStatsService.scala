package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkerAssignmentUpdate,
  WorkflowCompleted,
  WorkflowRecoveryStatus,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.Utils
import edu.uci.ics.texera.web.SubscriptionManager
import edu.uci.ics.texera.web.model.websocket.event.{
  ExecutionDurationUpdateEvent,
  OperatorStatistics,
  OperatorStatisticsUpdateEvent,
  WorkerAssignmentUpdateEvent
}
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.storage.JobStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.OperatorWorkerMapping
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{FAILED, COMPLETED}

class JobStatsService(
    client: AmberClient,
    stateStore: JobStateStore
) extends SubscriptionManager {

  registerCallbacks()

  addSubscription(
    stateStore.statsStore.registerDiffHandler((oldState, newState) => {
      // Update operator stats if any operator updates its stat
      if (newState.operatorInfo.toSet != oldState.operatorInfo.toSet) {
        Iterable(
          OperatorStatisticsUpdateEvent(newState.operatorInfo.collect {
            case x =>
              val stats = x._2
              val res = OperatorStatistics(
                Utils.aggregatedStateToString(stats.state),
                stats.inputCount,
                stats.outputCount
              )
              (x._1, res)
          })
        )
      } else {
        Iterable.empty
      }
    })
  )

  addSubscription(
    stateStore.statsStore.registerDiffHandler((oldState, newState) => {
      // update operators' workers.
      if (newState.operatorWorkerMapping != oldState.operatorWorkerMapping) {
        newState.operatorWorkerMapping
          .map { opToWorkers =>
            WorkerAssignmentUpdateEvent(opToWorkers.operatorId, opToWorkers.workerIds)
          }
      } else {
        Iterable()
      }
    })
  )

  addSubscription(
    stateStore.statsStore.registerDiffHandler((oldState, newState) => {
      // update execution duration.
      if (
        newState.startTimeStamp != oldState.startTimeStamp || newState.endTimeStamp != oldState.endTimeStamp
      ) {
        if (newState.endTimeStamp != 0) {
          Iterable(
            ExecutionDurationUpdateEvent(
              newState.endTimeStamp - newState.startTimeStamp,
              isRunning = false
            )
          )
        } else {
          val currentTime = System.currentTimeMillis()
          Iterable(
            ExecutionDurationUpdateEvent(currentTime - newState.startTimeStamp, isRunning = true)
          )
        }
      } else {
        Iterable()
      }
    })
  )

  private[this] def registerCallbacks(): Unit = {
    registerCallbackOnWorkflowStatusUpdate()
    registerCallbackOnWorkerAssignedUpdate()
    registerCallbackOnWorkflowRecoveryUpdate()
    registerCallbackOnWorkflowComplete()
    registerCallbackOnFatalError()
  }

  private[this] def registerCallbackOnWorkflowStatusUpdate(): Unit = {
    addSubscription(
      client
        .registerCallback[WorkflowStatusUpdate]((evt: WorkflowStatusUpdate) => {
          stateStore.statsStore.updateState { jobInfo =>
            jobInfo.withOperatorInfo(evt.operatorStatistics)
          }
        })
    )
  }

  private[this] def registerCallbackOnWorkerAssignedUpdate(): Unit = {
    addSubscription(
      client
        .registerCallback[WorkerAssignmentUpdate]((evt: WorkerAssignmentUpdate) => {
          stateStore.statsStore.updateState { jobInfo =>
            jobInfo.withOperatorWorkerMapping(
              evt.workerMapping
                .map({
                  case (opId, workerIds) => OperatorWorkerMapping(opId, workerIds)
                })
                .toSeq
            )
          }
        })
    )
  }

  private[this] def registerCallbackOnWorkflowRecoveryUpdate(): Unit = {
    addSubscription(
      client
        .registerCallback[WorkflowRecoveryStatus]((evt: WorkflowRecoveryStatus) => {
          stateStore.jobMetadataStore.updateState { jobMetadata =>
            jobMetadata.withIsRecovering(evt.isRecovering)
          }
        })
    )
  }

  private[this] def registerCallbackOnWorkflowComplete(): Unit = {
    addSubscription(
      client
        .registerCallback[WorkflowCompleted]((evt: WorkflowCompleted) => {
          client.shutdown()
          stateStore.statsStore.updateState(stats =>
            stats.withEndTimeStamp(System.currentTimeMillis())
          )
          stateStore.jobMetadataStore.updateState(jobInfo =>
            updateWorkflowState(COMPLETED, jobInfo)
          )
        })
    )
  }

  private[this] def registerCallbackOnFatalError(): Unit = {
    addSubscription(
      client
        .registerCallback[FatalError]((evt: FatalError) => {
          client.shutdown()
          stateStore.statsStore.updateState(stats =>
            stats.withEndTimeStamp(System.currentTimeMillis())
          )
          stateStore.jobMetadataStore.updateState { jobInfo =>
            updateWorkflowState(FAILED, jobInfo).withError(evt.e.getLocalizedMessage)
          }
        })
    )
  }
}
