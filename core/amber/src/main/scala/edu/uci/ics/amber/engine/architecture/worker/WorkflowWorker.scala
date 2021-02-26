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
import edu.uci.ics.amber.engine.recovery.DataLogManager.DataLogElement
import edu.uci.ics.amber.engine.recovery.{DPLogManager, DataLogManager, EmptyLogStorage, LogStorage}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      op: IOperatorExecutor,
      parentNetworkCommunicationActorRef: ActorRef,
      controlLogStorage: LogStorage[WorkflowControlMessage] = new EmptyLogStorage(),
      dataLogStorage: LogStorage[DataLogElement] = new EmptyLogStorage(),
      dpLogStorage: LogStorage[Long] = new EmptyLogStorage()
  ): Props =
    Props(
      new WorkflowWorker(
        id,
        op,
        parentNetworkCommunicationActorRef,
        controlLogStorage,
        dataLogStorage,
        dpLogStorage
      )
    )
}

class WorkflowWorker(
    identifier: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef,
    controlLogStorage: LogStorage[WorkflowControlMessage] = new EmptyLogStorage(),
    dataLogStorage: LogStorage[DataLogElement] = new EmptyLogStorage(),
    dpLogStorage: LogStorage[Long] = new EmptyLogStorage()
) extends WorkflowActor(identifier, parentNetworkCommunicationActorRef, controlLogStorage) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  val workerStateManager: WorkerStateManager = new WorkerStateManager()

  lazy val dataLogManager: DataLogManager = wire[DataLogManager]
  lazy val dpLogManager: DPLogManager = wire[DPLogManager]

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

  if (parentNetworkCommunicationActorRef != null) {
    parentNetworkCommunicationActorRef ! RegisterActorRef(identifier, self)
  }

  workerStateManager.assertState(Uninitialized)
  workerStateManager.transitTo(Ready)

  dataLogManager.onComplete(() => {
    context.become(receiveAndProcessMessages)
    unstashAll()
  })

  def recovering: Receive = {
    disallowActorRefRelatedMessages orElse
      receiveDataMessagesDuringRecovery orElse
      stashControlMessages orElse
      logUnhandledMessages
  }

  override def receive: Receive = recovering

  def receiveAndProcessMessages: Receive = {
    disallowActorRefRelatedMessages orElse
      processControlMessages orElse
      receiveDataMessages orElse
      logUnhandledMessages
  }

  final def receiveDataMessages: Receive = {
    case msg @ NetworkMessage(id, data: WorkflowDataMessage) =>
      sender ! NetworkAck(id)
      dataInputPort.handleDataMessage(data)
  }

  final def receiveDataMessagesDuringRecovery: Receive = {
    case msg @ NetworkMessage(id, data: WorkflowDataMessage) =>
      sender ! NetworkAck(id)
      dataInputPort.handleDataMessage(data)
  }

  override def postStop(): Unit = {
    // shutdown dp thread by sending a command
    dataProcessor.enqueueCommand(ShutdownDPThread(), ActorVirtualIdentity.Self)
    super.postStop()
  }

}
