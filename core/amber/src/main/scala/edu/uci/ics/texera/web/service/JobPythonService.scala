package edu.uci.ics.texera.web.service

import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.PythonPrintTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput, WorkflowStateStore}
import edu.uci.ics.texera.web.model.websocket.event.{BreakpointTriggeredEvent, TexeraWebSocketEvent}
import edu.uci.ics.texera.web.model.websocket.event.python.PythonPrintTriggeredEvent
import edu.uci.ics.texera.web.model.websocket.request.{RetryRequest, SkipTupleRequest}
import edu.uci.ics.texera.web.service.JobPythonService.bufferSize
import edu.uci.ics.texera.web.workflowruntimestate.PythonOperatorInfo
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{RESUMING, RUNNING}

object JobPythonService {
  val bufferSize: Int = AmberUtils.amberConfig.getInt("web-server.python-console-buffer-size")

}

class JobPythonService(
    client: AmberClient,
    stateStore: WorkflowStateStore,
    wsInput: WebsocketInput,
    breakpointService: JobBreakpointService
) extends SubscriptionManager {
  registerCallbackOnPythonPrint()

  addSubscription(
    stateStore.pythonStore.registerDiffHandler((oldState, newState) => {
      // For each operator, check if it has new python console message or breakpoint events
      newState.operatorInfo
        .collect {
          case (opId, info) =>
            val oldInfo = oldState.operatorInfo.getOrElse(opId, new PythonOperatorInfo())
            if (info.consoleMessages.nonEmpty && info.consoleMessages != oldInfo.consoleMessages) {
              val stringBuilder = new StringBuilder()
              info.consoleMessages.foreach(s => stringBuilder.append(s))
              PythonPrintTriggeredEvent(stringBuilder.toString(), opId)
            }
        }
        .asInstanceOf[Iterable[TexeraWebSocketEvent]]
    })
  )

  private[this] def registerCallbackOnPythonPrint(): Unit = {
    addSubscription(
      client
        .registerCallback[PythonPrintTriggered]((evt: PythonPrintTriggered) => {
          stateStore.pythonStore.updateState { jobInfo =>
            val opInfo = jobInfo.operatorInfo.getOrElse(evt.operatorID, PythonOperatorInfo())
            if (opInfo.consoleMessages.size < bufferSize) {
              jobInfo.addOperatorInfo((evt.operatorID, opInfo.addConsoleMessages(evt.message)))
            } else {
              jobInfo.addOperatorInfo(
                (
                  evt.operatorID,
                  opInfo.withConsoleMessages(
                    opInfo.consoleMessages.drop(1) :+ evt.message
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
    val f = client.sendAsync(RetryWorkflow())
    stateStore.jobStateStore.updateState(jobInfo => jobInfo.withState(RESUMING))
    f.onSuccess { _ =>
      stateStore.jobStateStore.updateState(jobInfo => jobInfo.withState(RUNNING))
    }
  }))

}
