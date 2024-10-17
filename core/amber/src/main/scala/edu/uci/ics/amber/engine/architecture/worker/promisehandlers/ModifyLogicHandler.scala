package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.UpdateExecutorCompleted
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecInitInfo, OpExecInitInfoWithCode, OpExecInitInfoWithFunc}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, ModifyLogicRequest, UpdateExecutorRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.{AmberRuntime, VirtualIdentityUtils}
import edu.uci.ics.texera.workflow.common.operators.StateTransferFunc

/** Get queue and other resource usage of this worker
  *
  * possible sender: controller(by ControllerInitiateMonitoring)
  */
trait ModifyLogicHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def modifyLogic(msg: ModifyLogicRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
    msg.updateRequest.find(_.targetOpId == VirtualIdentityUtils.getPhysicalOpId(this.actorId)) match {
      case Some(value) =>
        performUpdateExecutor(value)
        sendToClient(UpdateExecutorCompleted(this.actorId))
      case None =>
        // do nothing
    }
    EmptyReturn()
  }

  private def performUpdateExecutor(req: UpdateExecutorRequest): Unit = {
    val oldOpExecState = dp.executor
    val bytes = req.newExecutor.value.toByteArray
    val opExecInitInfo: OpExecInitInfo = AmberRuntime.serde.deserialize(bytes, classOf[OpExecInitInfo]).get
    dp.executor = opExecInitInfo match {
      case OpExecInitInfoWithCode(codeGen) =>
        ??? // TODO: compile and load java/scala operator here
      case OpExecInitInfoWithFunc(opGen) =>
        opGen(VirtualIdentityUtils.getWorkerIndex(actorId), 1)
    }

    if (req.stateTransferFunc.nonEmpty) {
      val bytesForStateTransfer = req.stateTransferFunc.get.value.toByteArray
      val stateTransferFunc: StateTransferFunc = AmberRuntime.serde.deserialize(bytesForStateTransfer, classOf[StateTransferFunc]).get
      stateTransferFunc.apply(oldOpExecState, dp.executor)
    }
    dp.executor.open()
  }

}
