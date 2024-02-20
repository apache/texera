package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.common.{
  AkkaActorRefMappingService,
  AkkaActorService,
  AkkaMessageTransferService,
  AmberProcessor
}
import edu.uci.ics.amber.engine.architecture.controller.execution.WorkflowExecution
import edu.uci.ics.amber.engine.architecture.logreplay.ReplayLogManager
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import edu.uci.ics.amber.engine.architecture.scheduling.{
  ExpansionGreedyRegionPlanGenerator,
  WorkflowExecutionController
}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

class ControllerProcessor(
    val workflowContext: WorkflowContext,
    var physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage,
    controllerConfig: ControllerConfig,
    actorId: ActorVirtualIdentity,
    outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit
) extends AmberProcessor(actorId, outputHandler) {

  val workflowExecution: WorkflowExecution = WorkflowExecution()

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

  var workflowExecutionController: WorkflowExecutionController = _

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

  def initWorkflowExecutionController(): Unit = {
    // generate an RegionPlan with regions.
    //  currently, ExpansionGreedyRegionPlanGenerator is the only RegionPlan generator.
    val (regionPlan, updatedPhysicalPlan) = new ExpansionGreedyRegionPlanGenerator(
      workflowContext,
      physicalPlan,
      opResultStorage
    ).generate()
    this.physicalPlan = updatedPhysicalPlan

    this.workflowExecutionController = new WorkflowExecutionController(
      regionPlan,
      workflowExecution,
      controllerConfig,
      asyncRPCClient
    )
  }

}
