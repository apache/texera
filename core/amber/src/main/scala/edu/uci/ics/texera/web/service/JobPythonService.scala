package edu.uci.ics.texera.web.service

import com.twitter.util.{Await, Duration}
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.ConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EvaluatePythonExpressionHandler.EvaluatePythonExpression
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.event.python.ConsoleUpdateEvent
import edu.uci.ics.texera.web.model.websocket.request.RetryRequest
import edu.uci.ics.texera.web.model.websocket.request.python.PythonExpressionEvaluateRequest
import edu.uci.ics.texera.web.model.websocket.response.python.PythonExpressionEvaluateResponse
import edu.uci.ics.texera.web.service.JobPythonService.bufferSize
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{RESUMING, RUNNING}
import edu.uci.ics.texera.web.workflowruntimestate.{EvaluatedValueList, PythonOperatorInfo}
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
            val diff = info.consoleMessages.diff(oldInfo.consoleMessages)
            output.append(ConsoleUpdateEvent(opId, diff))

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
        .registerCallback[ConsoleMessageTriggered]((evt: ConsoleMessageTriggered) => {
          stateStore.pythonStore.updateState { jobInfo =>
            {
              if (!jobInfo.operatorInfo.contains(evt.operatorId)) {
                jobInfo.addOperatorInfo((evt.operatorId, PythonOperatorInfo()))
              }
              val opInfo = jobInfo.operatorInfo.getOrElse(evt.operatorId, PythonOperatorInfo())

              if (opInfo.consoleMessages.size < bufferSize) {
                jobInfo.addOperatorInfo(
                  (
                    evt.operatorId,
                    opInfo.addConsoleMessages(evt.consoleMessage)
                  )
                )
              } else {
                jobInfo.addOperatorInfo(
                  (
                    evt.operatorId,
                    opInfo.withConsoleMessages(opInfo.consoleMessages.drop(1) :+ evt.consoleMessage)
                  )
                )
              }
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
