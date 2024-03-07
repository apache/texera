package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{
  OpExecInitInfo,
  OpExecInitInfoWithCode,
  OpExecInitInfoWithFunc
}
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.common.VirtualIdentityUtils
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object InitializeOperatorLogicHandler {
  final case class InitializeOperatorLogic(
      totalWorkerCount: Int,
      opExecInitInfo: OpExecInitInfo,
      isSource: Boolean
  ) extends ControlCommand[Unit]
}

trait InitializeOperatorLogicHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: InitializeOperatorLogic, sender) =>
    {
      dp.operator = msg.opExecInitInfo match {
        case OpExecInitInfoWithCode(codeGen) =>
          ??? // TODO: compile and load java/scala operator here
        case OpExecInitInfoWithFunc(opGen) =>
          opGen(VirtualIdentityUtils.getWorkerIndex(actorId), msg.totalWorkerCount)
      }
    }
  }

}
