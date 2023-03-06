package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.worker.WorkerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.ModifyOperatorLogicHandler.{
  WorkerModifyLogic,
  WorkerModifyLogicComplete,
  WorkerModifyLogicMultiple
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc

object ModifyOperatorLogicHandler {
  case class WorkerModifyLogic(
      opExecConfig: OpExecConfig,
      stateTransferFunc: Option[StateTransferFunc]
  ) extends ControlCommand[Unit]

  case class WorkerModifyLogicMultiple(modifyLogicList: List[WorkerModifyLogic])
      extends ControlCommand[Unit]

  case class WorkerModifyLogicComplete(workerID: ActorVirtualIdentity) extends ControlCommand[Unit]
}

/** Get queue and other resource usage of this worker
  *
  * possible sender: controller(by ControllerInitiateMonitoring)
  */
trait ModifyOperatorLogicHandler {
  this: WorkerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: WorkerModifyLogic, _) =>
    performModifyLogic(msg)
    sendToClient(WorkerModifyLogicComplete(this.actorId))
  }

  registerHandler { (msg: WorkerModifyLogicMultiple, _) =>
    val modifyLogic =
      msg.modifyLogicList.find(o => o.opExecConfig.id == dataProcessor.opExecConfig.id)
    if (modifyLogic.nonEmpty) {
      performModifyLogic(modifyLogic.get)
      sendToClient(WorkerModifyLogicComplete(this.actorId))
    }
  }

  private def performModifyLogic(modifyLogic: WorkerModifyLogic): Unit = {
    val newOpExecConfig = modifyLogic.opExecConfig
    val newOperator =
      newOpExecConfig.initIOperatorExecutor((dataProcessor.workerIndex, newOpExecConfig))
    if (modifyLogic.stateTransferFunc.nonEmpty) {
      modifyLogic.stateTransferFunc.get.apply(dataProcessor.operator, newOperator)
    }
    dataProcessor.operator = newOperator
    this.operator = newOperator
    operator.open()
  }

}
