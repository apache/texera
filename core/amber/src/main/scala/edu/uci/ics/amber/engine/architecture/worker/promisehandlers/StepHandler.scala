package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.{DataProcessorRPCHandlerInitializer, DebuggerPause}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StepHandler.{ContinueProcessing, StopProcessing}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object StepHandler {
  final case class ContinueProcessing(stepSize:Option[Int] = None) extends ControlCommand[Unit]
  final case class StopProcessing() extends ControlCommand[Unit]
}

trait StepHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: ContinueProcessing, sender) =>
    logger.info(s"received $msg")
    if(msg.stepSize.isEmpty){
      dp.pauseManager.resume(DebuggerPause)
    }else{
      dp.pauseManager.allowedProcessedTuples = msg.stepSize
    }
  }

  registerHandler { (msg: StopProcessing, sender) =>
    logger.info(s"received $msg")
    dp.pauseManager.pause(DebuggerPause)
  }

}
