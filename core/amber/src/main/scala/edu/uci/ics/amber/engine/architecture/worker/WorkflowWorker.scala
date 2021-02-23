package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.AbstractActor.ActorContext
import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.WorkerExecutionStartedHandler.WorkerStateUpdated
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.DataInputPort.WorkflowDataMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage,
  RegisterActorRef
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  ControlInputPort,
  DataInputPort,
  DataOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.UnblockForControlCommands
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ShutdownDPThreadHandler.ShutdownDPThread
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCClient,
  AsyncRPCHandlerInitializer,
  AsyncRPCServer
}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager._
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.{
  IOperatorExecutor,
  ISourceOperatorExecutor,
  ITupleSinkOperatorExecutor
}
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.annotation.elidable
import scala.annotation.elidable.INFO
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
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  val workerStateManager: WorkerStateManager = new WorkerStateManager()

  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: DataInputPort = wire[DataInputPort]
  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  lazy val batchProducer: TupleToBatchConverter = wire[TupleToBatchConverter]
  lazy val tupleProducer: BatchToTupleConverter = wire[BatchToTupleConverter]
  lazy val breakpointManager: BreakpointManager = wire[BreakpointManager]

  override lazy val controlInputPort: ControlInputPort = wire[WorkerControlInputPort]

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
    disallowActorRefRelatedMessages orElse
      processControlMessages orElse
      receiveDataMessages orElse {
      case other =>
        logger.logError(
          WorkflowRuntimeError(s"unhandled message: $other", identifier.toString, Map.empty)
        )
    }
  }

  final def receiveDataMessages: Receive = {
    case msg @ NetworkMessage(id, data: WorkflowDataMessage) =>
      if (workerStateManager.getCurrentState == Ready) {
        workerStateManager.transitTo(Running)
        asyncRPCClient.send(
          WorkerStateUpdated(workerStateManager.getCurrentState),
          ActorVirtualIdentity.Controller
        )
      }
      sender ! NetworkAck(id)
      dataInputPort.handleDataMessage(data)
  }

  override def postStop(): Unit = {
    // shutdown dp thread by sending a command
    dataProcessor.enqueueCommand(
      ControlInvocation(AsyncRPCClient.IgnoreReply, ShutdownDPThread()),
      ActorVirtualIdentity.Self
    )
    logger.logInfo("stopped!")
  }

}
