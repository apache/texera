package edu.uci.ics.amber.engine.common.ambermessage2

import edu.uci.ics.amber.engine.architecture.worker.controlcommands.{
  ControlCommand,
  ControlCommandConvertUtils
}

object InvocationConvertUtils {

  def controlInvocationToV2(
      controlInvocation: edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
  ): ControlInvocation = {

    val commandV2: ControlCommand =
      ControlCommandConvertUtils.controlCommandToV2(controlInvocation.command)
    ControlInvocation(controlInvocation.commandID, commandV2)
  }

  def controlInvocationToV1(
      controlInvocation: ControlInvocation
  ): edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation = {
    val commandV1: edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand[_] =
      ControlCommandConvertUtils.controlCommandToV1(controlInvocation.command)
    edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
      .ControlInvocation(controlInvocation.commandId, commandV1)
  }

  def returnInvocationToV1(
      returnInvocation: ReturnInvocation
  ): edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ReturnInvocation =
    edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
      .ReturnInvocation(returnInvocation.originalCommandId, returnInvocation.controlReturn)

}
