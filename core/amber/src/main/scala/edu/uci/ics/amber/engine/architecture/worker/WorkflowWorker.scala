package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.ExecutionStartedHandler.ExecutionStarted
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.DataInputPort.WorkflowDataMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{NetworkAck, NetworkMessage, RegisterActorRef}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{BatchToTupleConverter, DataInputPort, DataOutputPort, TupleToBatchConverter}
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{EndMarker, EndOfAllMarker, InputTuple}
import edu.uci.ics.amber.engine.architecture.worker.neo._
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler.PauseWorker
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCHandlerInitializer, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager._
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, ISourceOperatorExecutor, ITupleSinkOperatorExecutor}
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

  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: DataInputPort = wire[DataInputPort]
  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  lazy val batchProducer: TupleToBatchConverter = wire[TupleToBatchConverter]
  lazy val tupleProducer: BatchToTupleConverter = wire[BatchToTupleConverter]
  lazy val workerStateManager: WorkerStateManager = wire[WorkerStateManager]
  lazy val breakpointManager: BreakpointManager = wire[BreakpointManager]

  val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[WorkerAsyncRPCHandlerInitializer]

  val receivedFaultedTupleIds: mutable.HashSet[Long] = new mutable.HashSet[Long]()
  var isCompleted = false

  if(parentNetworkCommunicationActorRef != null){
    parentNetworkCommunicationActorRef ! RegisterActorRef(identifier, self)
  }

  workerStateManager.confirmState(Uninitialized)
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
        asyncRPCClient.send(ExecutionStarted(), VirtualIdentity.Controller)
      }
      sender ! NetworkAck(id)
      dataInputPort.handleDataMessage(data)
  }

}
