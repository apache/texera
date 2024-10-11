package edu.uci.ics.amber.engine.architecture.controller

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.common.{AkkaActorRefMappingService, AkkaActorService, AkkaMessageTransferService, AmberProcessor}
import edu.uci.ics.amber.engine.architecture.controller.execution.WorkflowExecution
import edu.uci.ics.amber.engine.architecture.logreplay.ReplayLogManager
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AsyncRPCContext
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceFs2Grpc
import edu.uci.ics.amber.engine.architecture.rpc.testerservice.RPCTesterFs2Grpc
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowExecutionCoordinator
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage

class ControllerProcessor(
    workflowContext: WorkflowContext,
    opResultStorage: OpResultStorage,
    controllerConfig: ControllerConfig,
    actorId: ActorVirtualIdentity,
    outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit
) extends AmberProcessor(actorId, outputHandler) {

  val asyncRPCClient = new AsyncRPCClient[ControllerServiceFs2Grpc[Future, AsyncRPCContext]](outputGateway, actorId)

  val workflowExecution: WorkflowExecution = WorkflowExecution()
  val workflowScheduler: WorkflowScheduler = new WorkflowScheduler(workflowContext, opResultStorage)
  val workflowExecutionCoordinator: WorkflowExecutionCoordinator = new WorkflowExecutionCoordinator(
    () => this.workflowScheduler.getNextRegions,
    workflowExecution,
    controllerConfig,
    asyncRPCClient
  )

  private val initializer = new ControllerAsyncRPCHandlerInitializer(this)

  @transient var controllerTimerService: ControllerTimerService = _
  def setupTimerService(controllerTimerService: ControllerTimerService): Unit = {
    this.controllerTimerService = controllerTimerService
  }

  @transient var transferService: AkkaMessageTransferService = _
  def setupTransferService(transferService: AkkaMessageTransferService): Unit = {
    this.transferService = transferService
  }

  @transient var actorService: AkkaActorService = _

  def setupActorService(akkaActorService: AkkaActorService): Unit = {
    this.actorService = akkaActorService
  }

  @transient var actorRefService: AkkaActorRefMappingService = _
  def setupActorRefService(actorRefService: AkkaActorRefMappingService): Unit = {
    this.actorRefService = actorRefService
  }

  @transient var logManager: ReplayLogManager = _

  def setupLogManager(logManager: ReplayLogManager): Unit = {
    this.logManager = logManager
  }

}
