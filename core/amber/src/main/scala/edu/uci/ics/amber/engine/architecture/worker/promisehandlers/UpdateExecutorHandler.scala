package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.UpdateExecutorCompleted
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecInitInfoWithCode, OpExecInitInfoWithFunc}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, UpdateExecutorRequest, UpdateMultipleExecutorsRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils

/** Get queue and other resource usage of this worker
  *
  * possible sender: controller(by ControllerInitiateMonitoring)
  */
trait UpdateExecutorHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def updateExecutor(msg: UpdateExecutorRequest, ctx: AsyncRPCContext): Future[Empty] = {
    performUpdateExecutor(msg)
    sendToClient(UpdateExecutorCompleted(this.actorId))
    Empty()
  }

  override def updateMultipleExecutors(msg: UpdateMultipleExecutorsRequest, ctx: AsyncRPCContext): Future[Empty] = {
    msg.executorsToUpdate
      .find(_.physicalOp == VirtualIdentityUtils.getPhysicalOpId(actorId))
      .foreach { executorToUpdate =>
        performUpdateExecutor(executorToUpdate)
        sendToClient(UpdateExecutorCompleted(this.actorId))
      }
    Empty()
  }

  private def performUpdateExecutor(updateExecutor: UpdateExecutorRequest): Unit = {
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
