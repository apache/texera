package edu.uci.ics.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.{Await, Duration}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ConsoleMessageHandler.ConsoleMessageTriggered
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.EvaluatePythonExpressionHandler.EvaluatePythonExpression
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.DebugCommandHandler.DebugCommand
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.RetryWorkflowHandler.RetryWorkflow
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.ConsoleMessage
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.ConsoleMessageType.COMMAND
import edu.uci.ics.amber.engine.common.{AmberConfig, VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.event.python.ConsoleUpdateEvent
import edu.uci.ics.texera.web.model.websocket.request.RetryRequest
import edu.uci.ics.texera.web.model.websocket.request.python.{
  DebugCommandRequest,
  PythonExpressionEvaluateRequest
}
import edu.uci.ics.texera.web.model.websocket.response.python.PythonExpressionEvaluateResponse
import edu.uci.ics.texera.web.storage.JobStateStore
import edu.uci.ics.texera.web.storage.JobStateStore.updateWorkflowState
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState.{RESUMING, RUNNING}
import edu.uci.ics.texera.web.workflowruntimestate.{
  EvaluatedValueList,
  JobConsoleStore,
  OperatorConsole
}
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput}

import java.time.Instant
import scala.collection.mutable

class JobConsoleService(
    client: AmberClient,
    stateStore: JobStateStore,
    wsInput: WebsocketInput,
    breakpointService: JobBreakpointService
) extends SubscriptionManager {
  registerCallbackOnPythonConsoleMessage()

  val bufferSize: Int = AmberConfig.operatorConsoleBufferSize

  addSubscription(
    stateStore.consoleStore.registerDiffHandler((oldState, newState) => {
      val output = new mutable.ArrayBuffer[TexeraWebSocketEvent]()
      // For each operator, check if it has new python console message or breakpoint events
      newState.operatorConsole
        .foreach {
          case (opId, info) =>
            val oldConsole = oldState.operatorConsole.getOrElse(opId, new OperatorConsole())
            val diff = info.consoleMessages.diff(oldConsole.consoleMessages)
            output.append(ConsoleUpdateEvent(opId, diff))

            info.evaluateExprResults.keys
              .filterNot(oldConsole.evaluateExprResults.contains)
              .foreach { key =>
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
          stateStore.consoleStore.updateState { jobInfo =>
            val opId =
              VirtualIdentityUtils.getOperator(ActorVirtualIdentity(evt.consoleMessage.workerId))
            addConsoleMessage(jobInfo, opId.operator, evt.consoleMessage)
          }
        })
    )

  }

  private[this] def addConsoleMessage(
      jobInfo: JobConsoleStore,
      opId: String,
      consoleMessage: ConsoleMessage
  ): JobConsoleStore = {
    val opInfo = jobInfo.operatorConsole.getOrElse(opId, OperatorConsole())

    if (opInfo.consoleMessages.size < bufferSize) {
      jobInfo.addOperatorConsole(
        (
          opId,
          opInfo.addConsoleMessages(consoleMessage)
        )
      )
    } else {
      jobInfo.addOperatorConsole(
        (
          opId,
          opInfo.withConsoleMessages(opInfo.consoleMessages.drop(1) :+ consoleMessage)
        )
      )
    }
  }

  //Receive retry request
  addSubscription(wsInput.subscribe((req: RetryRequest, uidOpt) => {
    stateStore.jobMetadataStore.updateState(jobInfo => updateWorkflowState(RESUMING, jobInfo))
    client.sendAsyncWithCallback[Unit](
      RetryWorkflow(req.workers.map(x => ActorVirtualIdentity(x))),
      _ => stateStore.jobMetadataStore.updateState(jobInfo => updateWorkflowState(RUNNING, jobInfo))
    )
  }))

  //Receive evaluate python expression
  addSubscription(wsInput.subscribe((req: PythonExpressionEvaluateRequest, uidOpt) => {
    val result = Await.result(
      client.sendAsync(EvaluatePythonExpression(req.expression, req.operatorId)),
      Duration.fromSeconds(10)
    )
    stateStore.consoleStore.updateState(consoleStore => {
      val opInfo = consoleStore.operatorConsole.getOrElse(req.operatorId, OperatorConsole())
      consoleStore.addOperatorConsole(
        (
          req.operatorId,
          opInfo.addEvaluateExprResults((req.expression, EvaluatedValueList(result)))
        )
      )
    })

    // TODO: remove the following hack after fixing the frontend
    // currently frontend is not prepared for re-receiving the eval-expr messages
    // so we add it to the state and remove it from the state immediately
    stateStore.consoleStore.updateState(consoleStore => {
      val opInfo = consoleStore.operatorConsole.getOrElse(req.operatorId, OperatorConsole())
      consoleStore.addOperatorConsole((req.operatorId, opInfo.clearEvaluateExprResults))
    })
  }))

  //Receive debug command
  addSubscription(wsInput.subscribe((req: DebugCommandRequest, uidOpt) => {
    stateStore.consoleStore.updateState { jobInfo =>
      val newMessage = new ConsoleMessage(
        req.workerId,
        Timestamp(Instant.now),
        COMMAND,
        "USER-" + uidOpt.getOrElse("UNKNOWN"),
        req.cmd,
        ""
      )
      addConsoleMessage(jobInfo, req.operatorId, newMessage)
    }

    client.sendAsync(DebugCommand(req.workerId, req.cmd))

  }))

}
