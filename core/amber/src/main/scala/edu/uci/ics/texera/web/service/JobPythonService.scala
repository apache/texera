package edu.uci.ics.texera.web.service

import com.twitter.util.{Await, Duration}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.PythonConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EvaluatePythonExpressionHandler.EvaluatePythonExpression
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.PythonConsoleMessageV2
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.event.python.PythonConsoleUpdateEvent
import edu.uci.ics.texera.web.model.websocket.request.RetryRequest
import edu.uci.ics.texera.web.model.websocket.request.python.PythonExpressionEvaluateRequest
import edu.uci.ics.texera.web.model.websocket.response.python.PythonExpressionEvaluateResponse
import edu.uci.ics.texera.web.service.JobPythonService.bufferSize
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{RESUMING, RUNNING}
import edu.uci.ics.texera.web.workflowruntimestate.{
  EvaluatedValueList,
  PythonOperatorInfo,
  PythonWorkerInfo
}
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput}

import scala.collection.mutable

object JobPythonService {
  val bufferSize: Int = AmberUtils.amberConfig.getInt("web-server.python-console-buffer-size")

}

class JobPythonService(
    client: AmberClient,
    stateStore: JobStateStore,
    wsInput: WebsocketInput,
    breakpointService: JobBreakpointService
) extends SubscriptionManager {
  registerCallbackOnPythonConsoleMessage()

  addSubscription(
    stateStore.pythonStore.registerDiffHandler((oldState, newState) => {
      val output = new mutable.ArrayBuffer[TexeraWebSocketEvent]()
      // For each operator, check if it has new python console message or breakpoint events
      newState.operatorInfo
        .foreach {
          case (opId, info) =>
            val oldInfo = oldState.operatorInfo.getOrElse(opId, new PythonOperatorInfo())
            if (info.workerInfo.nonEmpty) {
              info.workerInfo.foreach({
                case (workerId, workerInfo) =>
                  val diff = if (oldInfo.workerInfo.contains(workerId)) {
                    workerInfo.pythonConsoleMessage diff oldInfo
                      .workerInfo(workerId)
                      .pythonConsoleMessage
                  } else {
                    workerInfo.pythonConsoleMessage
                  }
                  if (diff.nonEmpty) {
                    diff.foreach(s =>
                      output.append(
                        PythonConsoleUpdateEvent(opId, workerId, s.timestamp, s.msgType, s.message)
                      )
                    )
                  }

              })

            }
            info.evaluateExprResults.keys.filterNot(oldInfo.evaluateExprResults.contains).foreach {
              key =>
                output.append(
                  PythonExpressionEvaluateResponse(key, info.evaluateExprResults(key).values)
                )
            }
        }
      output
    })
  )

  private[this] def registerCallbackOnPythonConsoleMessage(): Unit = {
    addSubscription(
      client
        .registerCallback[PythonConsoleMessageTriggered]((evt: PythonConsoleMessageTriggered) => {
          stateStore.pythonStore.updateState { jobInfo =>
            if (!jobInfo.operatorInfo.contains(evt.operatorId)) {
              jobInfo.addOperatorInfo((evt.operatorId, PythonOperatorInfo()))
            }
            val opInfo = jobInfo.operatorInfo.getOrElse(evt.operatorId, PythonOperatorInfo())
            if (!opInfo.workerInfo.contains(evt.workerId)) {
              opInfo.addWorkerInfo((evt.workerId, PythonWorkerInfo()))
            }
            val workerInfo = opInfo.workerInfo.getOrElse(evt.workerId, PythonWorkerInfo())
            val newMessage = new PythonConsoleMessageV2(
              evt.consoleMessage.timestamp,
              evt.consoleMessage.msgType,
              evt.consoleMessage.message
            )
            if (workerInfo.pythonConsoleMessage.size < bufferSize) {
              jobInfo.addOperatorInfo(
                (
                  evt.operatorId,
                  opInfo.addWorkerInfo(
                    (evt.workerId, workerInfo.addPythonConsoleMessage(newMessage))
                  )
                )
              )
            } else {
              jobInfo.addOperatorInfo(
                (
                  evt.operatorId,
                  opInfo.addWorkerInfo(
                    (
                      evt.workerId,
                      workerInfo.withPythonConsoleMessage(
                        workerInfo.pythonConsoleMessage.drop(1) :+ newMessage
                      )
                    )
                  )
                )
              )
            }

          }

        })
    )

  }

  //Receive retry request
  addSubscription(wsInput.subscribe((req: RetryRequest, uidOpt) => {
    breakpointService.clearTriggeredBreakpoints()
    stateStore.jobMetadataStore.updateState(jobInfo => jobInfo.withState(RESUMING))
    client.sendAsyncWithCallback[Unit](
      RetryWorkflow(),
      _ => stateStore.jobMetadataStore.updateState(jobInfo => jobInfo.withState(RUNNING))
    )
  }))

  //Receive evaluate python expression
  addSubscription(wsInput.subscribe((req: PythonExpressionEvaluateRequest, uidOpt) => {
    val result = Await.result(
      client.sendAsync(EvaluatePythonExpression(req.expression, req.operatorId)),
      Duration.fromSeconds(10)
    )
    stateStore.pythonStore.updateState(pythonStore => {
      val opInfo = pythonStore.operatorInfo.getOrElse(req.operatorId, PythonOperatorInfo())
      pythonStore.addOperatorInfo(
        (
          req.operatorId,
          opInfo.addEvaluateExprResults((req.expression, EvaluatedValueList(result)))
        )
      )
    })
    // TODO: remove the following hack after fixing the frontend
    // currently frontend is not prepared for re-receiving the eval-expr messages
    // so we add it to the state and remove it from the state immediately
    stateStore.pythonStore.updateState(pythonStore => {
      val opInfo = pythonStore.operatorInfo.getOrElse(req.operatorId, PythonOperatorInfo())
      pythonStore.addOperatorInfo((req.operatorId, opInfo.clearEvaluateExprResults))
    })
  }))

}
