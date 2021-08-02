package edu.uci.ics.amber.engine.common.ambermessage

import edu.uci.ics.amber.engine.architecture.worker.controlcommands.ControlCommandConvertUtils.{
  controlReturnToV1,
  controlReturnToV2
}
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.{
  ControlCommandConvertUtils,
  ControlCommandV2
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnInvocation}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand

object InvocationConvertUtils {

  def controlInvocationToV2(
      controlInvocation: ControlInvocation
  ): ControlInvocationV2 = {

    val commandV2: ControlCommandV2 =
      ControlCommandConvertUtils.controlCommandToV2(controlInvocation.command)
    ControlInvocationV2(controlInvocation.commandID, commandV2)
  }

  def controlInvocationToV1(
      controlInvocation: ControlInvocationV2
  ): ControlInvocation = {
    val commandV1: ControlCommand[_] =
      ControlCommandConvertUtils.controlCommandToV1(controlInvocation.command)
    ControlInvocation(controlInvocation.commandId, commandV1)
  }

  def returnInvocationToV1(
      returnInvocation: ReturnInvocationV2
  ): ReturnInvocation = {
    ReturnInvocation(
      returnInvocation.originalCommandId,
      controlReturnToV1(returnInvocation.controlReturn)
    )

  }

  def returnInvocationToV2(returnInvocation: ReturnInvocation): ReturnInvocationV2 = {
    ReturnInvocationV2(
      returnInvocation.originalCommandID,
      controlReturnToV2(returnInvocation.controlReturn)
    )
  }

}
