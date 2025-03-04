package edu.uci.ics.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.{Await, Duration}
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.ConsoleMessageType.COMMAND
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  ConsoleMessage,
  EvaluatePythonExpressionRequest,
  DebugCommandRequest => AmberDebugCommandRequest
}
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.client.AmberClient
import edu.uci.ics.amber.engine.common.executionruntimestate.{
  EvaluatedValueList,
  ExecutionConsoleStore,
  OperatorConsole
}
import edu.uci.ics.amber.util.VirtualIdentityUtils
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, OperatorIdentity}
import edu.uci.ics.texera.web.model.websocket.event.TexeraWebSocketEvent
import edu.uci.ics.texera.web.model.websocket.event.python.ConsoleUpdateEvent
import edu.uci.ics.texera.web.model.websocket.request.RetryRequest
import edu.uci.ics.texera.web.model.websocket.request.python.{
  DebugCommandRequest,
  PythonExpressionEvaluateRequest
}
import edu.uci.ics.texera.web.model.websocket.response.python.PythonExpressionEvaluateResponse
import edu.uci.ics.texera.web.storage.ExecutionStateStore
import edu.uci.ics.texera.web.{SubscriptionManager, WebsocketInput}
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.core.storage.result.ResultSchema
import edu.uci.ics.amber.core.storage.{DocumentFactory, VFSURIFactory}
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.engine.architecture.controller.ExecutionStateUpdate
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkflowAggregatedState.{
  COMPLETED,
  FAILED,
  KILLED
}
import edu.uci.ics.texera.web.resource.dashboard.user.workflow.WorkflowExecutionsResource

import java.util.concurrent.{ExecutorService, Executors}
import java.time.Instant
import scala.collection.mutable

class ExecutionConsoleService(
    client: AmberClient,
    stateStore: ExecutionStateStore,
    wsInput: WebsocketInput,
    workflowContext: WorkflowContext
) extends SubscriptionManager
    with LazyLogging {
  registerCallbackOnPythonConsoleMessage()

  val bufferSize: Int = AmberConfig.operatorConsoleBufferSize

  private val consoleMessageOpIdToWriterMap: mutable.Map[String, BufferedItemWriter[Tuple]] =
    mutable.Map()

  private val consoleWriterThread: Option[ExecutorService] =
    Option.when(AmberConfig.isUserSystemEnabled)(Executors.newSingleThreadExecutor())

  private def getOrCreateWriter(opId: OperatorIdentity): BufferedItemWriter[Tuple] = {
    consoleMessageOpIdToWriterMap.getOrElseUpdate(
      opId.id, {
        val uri = VFSURIFactory
          .createConsoleMessagesURI(workflowContext.workflowId, workflowContext.executionId, opId)
        val writer = DocumentFactory
          .createDocument(uri, ResultSchema.consoleMessagesSchema)
          .writer("console_messages")
          .asInstanceOf[BufferedItemWriter[Tuple]]
        WorkflowExecutionsResource.insertOperatorExecutions(
          workflowContext.executionId.id,
          opId.id,
          uri
        )
        writer.open()
        writer
      }
    )
  }

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
        .registerCallback[ConsoleMessage]((evt: ConsoleMessage) => {
          stateStore.consoleStore.updateState { consoleStore =>
            val opId =
              VirtualIdentityUtils.getPhysicalOpId(
                ActorVirtualIdentity(evt.workerId)
              )
            addConsoleMessage(consoleStore, opId.logicalOpId.id, evt)
          }
        })
    )

  }

  addSubscription(
    client.registerCallback[ExecutionStateUpdate] {
      case ExecutionStateUpdate(state: WorkflowAggregatedState.Recognized)
          if Set(COMPLETED, FAILED, KILLED).contains(state) =>
        logger.info("Workflow execution terminated. Commit console messages.")
        consoleMessageOpIdToWriterMap.values.foreach { writer =>
          try {
            writer.close()
          } catch {
            case e: Exception =>
              logger.error("Failed to close console message writer", e)
          }
        }
      case _ =>
    }
  )

  private[this] def addConsoleMessage(
      consoleStore: ExecutionConsoleStore,
      opId: String,
      consoleMessage: ConsoleMessage
  ): ExecutionConsoleStore = {
    consoleWriterThread.foreach { thread =>
      thread.execute(() => {
        val writer = getOrCreateWriter(OperatorIdentity(opId))
        try {
          val tuple = new Tuple(
            ResultSchema.consoleMessagesSchema,
            Array(consoleMessage.toProtoString)
          )
          writer.putOne(tuple)
        } catch {
          case e: Exception =>
            logger.error(s"Error while writing console message for operator $opId", e)
        }
      })
    }

    val opInfo = consoleStore.operatorConsole.getOrElse(opId, OperatorConsole())

    if (opInfo.consoleMessages.size < bufferSize) {
      consoleStore.addOperatorConsole(
        (
          opId,
          opInfo.addConsoleMessages(consoleMessage)
        )
      )
    } else {
      consoleStore.addOperatorConsole(
        (
          opId,
          opInfo.withConsoleMessages(opInfo.consoleMessages.drop(1) :+ consoleMessage)
        )
      )
    }
  }

  //Receive retry request
  addSubscription(wsInput.subscribe((req: RetryRequest, uidOpt) => {
    // empty implementation
  }))

  //Receive evaluate python expression
  addSubscription(wsInput.subscribe((req: PythonExpressionEvaluateRequest, uidOpt) => {
    val result = Await.result(
      client.controllerInterface.evaluatePythonExpression(
        EvaluatePythonExpressionRequest(req.expression, req.operatorId),
        ()
      ),
      Duration.fromSeconds(10)
    )
    stateStore.consoleStore.updateState(consoleStore => {
      val opInfo = consoleStore.operatorConsole.getOrElse(req.operatorId, OperatorConsole())
      consoleStore.addOperatorConsole(
        (
          req.operatorId,
          opInfo.addEvaluateExprResults((req.expression, EvaluatedValueList(result.values)))
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
    stateStore.consoleStore.updateState { consoleStore =>
      val newMessage = new ConsoleMessage(
        req.workerId,
        Timestamp(Instant.now),
        COMMAND,
        "USER-" + uidOpt.getOrElse("UNKNOWN"),
        req.cmd,
        ""
      )
      addConsoleMessage(consoleStore, req.operatorId, newMessage)
    }

    client.controllerInterface.debugCommand(AmberDebugCommandRequest(req.workerId, req.cmd), ())

  }))

}
