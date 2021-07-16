package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  DataOutputPort,
  NetworkInputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShutdownDPThreadHandler.ShutdownDPThread
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlPayload,
  DataPayload,
  WorkflowControlMessage,
  WorkflowDataMessage
}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager._
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.{CONTROLLER, SELF}
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      op: IOperatorExecutor,
      parentNetworkCommunicationActorRef: ActorRef
  ): Props =
    Props(new WorkflowWorker(id, op, parentNetworkCommunicationActorRef))
}

class WorkflowWorker(
    identifier: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(identifier, parentNetworkCommunicationActorRef) {
  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: NetworkInputPort[DataPayload] =
    new NetworkInputPort[DataPayload](this.logger, this.handleDataPayload)
  lazy val controlInputPort: NetworkInputPort[ControlPayload] =
    new NetworkInputPort[ControlPayload](this.logger, this.handleControlPayload)
  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  lazy val batchProducer: TupleToBatchConverter = wire[TupleToBatchConverter]
  lazy val tupleProducer: BatchToTupleConverter = wire[BatchToTupleConverter]
  lazy val breakpointManager: BreakpointManager = wire[BreakpointManager]
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds
  val workerStateManager: WorkerStateManager = new WorkerStateManager()
  val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[WorkerAsyncRPCHandlerInitializer]

  val receivedFaultedTupleIds: mutable.HashSet[Long] = new mutable.HashSet[Long]()
  var isCompleted = false

  if (parentNetworkCommunicationActorRef != null) {
    parentNetworkCommunicationActorRef ! RegisterActorRef(identifier, self)
  }

  workerStateManager.assertState(Uninitialized)
  workerStateManager.transitTo(Ready)

  override def receive: Receive = receiveAndProcessMessages

  def receiveAndProcessMessages: Receive = {
    disallowActorRefRelatedMessages orElse {
      case NetworkMessage(id, WorkflowDataMessage(from, seqNum, payload)) =>
        dataInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
      case NetworkMessage(id, WorkflowControlMessage(from, seqNum, payload)) =>
        controlInputPort.handleMessage(this.sender(), id, from, seqNum, payload)
      case other =>
        logger.logError(
          WorkflowRuntimeError(s"unhandled message: $other", identifier.toString, Map.empty)
        )
    }
  }

  final def handleDataPayload(from: ActorVirtualIdentity, dataPayload: DataPayload): Unit = {
    if (workerStateManager.getCurrentState == Ready) {
      workerStateManager.transitTo(Running)
      asyncRPCClient.send(
        WorkerStateUpdated(workerStateManager.getCurrentState),
        CONTROLLER
      )
    }
    tupleProducer.processDataPayload(from, dataPayload)
  }

  final def handleControlPayload(
      from: ActorVirtualIdentity,
      controlPayload: ControlPayload
  ): Unit = {
    // let dp thread process it
    assert(from.isInstanceOf[ActorVirtualIdentity])
    controlPayload match {
      case controlCommand @ (ControlInvocation(_, _) | ReturnPayload(_, _)) =>
        dataProcessor.enqueueCommand(controlCommand, from)
      case _ =>
        logger.logError(
          WorkflowRuntimeError(
            s"unhandled control payload: $controlPayload",
            identifier.toString,
            Map.empty
          )
        )
    }
  }

  override def postStop(): Unit = {
    // shutdown dp thread by sending a command
    dataProcessor.enqueueCommand(
      ControlInvocation(AsyncRPCClient.IgnoreReply, ShutdownDPThread()),
      SELF
    )
    logger.logInfo("stopped!")
  }

}
