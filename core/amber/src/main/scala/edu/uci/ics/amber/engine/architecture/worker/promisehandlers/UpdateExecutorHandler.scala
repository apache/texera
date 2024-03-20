package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  OpExecInitInfoWithCode,
  OpExecInitInfoWithFunc
}
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateExecutorHandler.{
  UpdateExecutor,
  UpdateExecutorCompleted,
  UpdateMultipleExecutors
}
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc

object UpdateExecutorHandler {
  case class UpdateExecutor(
      physicalOp: PhysicalOp,
      stateTransferFunc: Option[StateTransferFunc]
  ) extends ControlCommand[Unit]

  case class UpdateMultipleExecutors(executorsToUpdate: List[UpdateExecutor])
      extends ControlCommand[Unit]

  case class UpdateExecutorCompleted(workerId: ActorVirtualIdentity) extends ControlCommand[Unit]
}

/** Get queue and other resource usage of this worker
  *
  * possible sender: controller(by ControllerInitiateMonitoring)
  */
trait UpdateExecutorHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: UpdateExecutor, _) =>
    performUpdateExecutor(msg)
    sendToClient(UpdateExecutorCompleted(this.actorId))
  }

  registerHandler { (msg: UpdateMultipleExecutors, _) =>
    msg.executorsToUpdate
      .find(_.physicalOp.id == VirtualIdentityUtils.getPhysicalOpId(actorId))
      .foreach { executorToUpdate =>
        performUpdateExecutor(executorToUpdate)
        sendToClient(UpdateExecutorCompleted(this.actorId))
      }
  }

  private def performUpdateExecutor(updateExecutor: UpdateExecutor): Unit = {
    val oldOpExecState = dp.executor
    dp.executor = updateExecutor.physicalOp.opExecInitInfo match {
      case OpExecInitInfoWithCode(codeGen) =>
        ??? // TODO: compile and load java/scala operator here
      case OpExecInitInfoWithFunc(opGen) =>
        opGen(VirtualIdentityUtils.getWorkerIndex(actorId), 1)
    }

    if (updateExecutor.stateTransferFunc.nonEmpty) {
      updateExecutor.stateTransferFunc.get.apply(oldOpExecState, dp.executor)
    }
    dp.executor.open()
  }

}
