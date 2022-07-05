package edu.uci.ics.amber.engine.architecture.logging

import edu.uci.ics.amber.engine.architecture.logging.determinants.{ControlDeterminant, DataOrderDeterminant, Determinant}
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.ControlCommandConvertUtils
import edu.uci.ics.amber.engine.common.ambermessage.{ControlInvocationV2, ControlPayload, ReturnInvocationV2}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class SerializationManager(determinantLogWriter: DeterminantLogWriter) {

  def compressedWrite(allDeterminants:Iterable[InMemDeterminant]):Unit= {
    var sender:ActorVirtualIdentity = null
    var count = 0
    allDeterminants.foreach {
      case pcm: ProcessControlMessage =>
        determinantLogWriter.writeLogRecord(serializeControl(pcm))
      case SenderChangeTo(actorVirtualIdentity) =>
        if (sender != null) {
          determinantLogWriter.writeLogRecord(DataOrderDeterminant(sender, count).toByteArray)
        }
        sender = actorVirtualIdentity
        count = 1
      case ProcessDataTuple =>
        count += 1
    }
  }

  def serializeControl(control: ProcessControlMessage):Array[Byte] = {
    try {
      val payloadV2 = control.controlPayload match {
        case invocation: AsyncRPCClient.ControlInvocation =>
          val cmdV2 = ControlCommandConvertUtils.controlCommandToV2(invocation.command)
          ControlInvocationV2(invocation.commandID, cmdV2)
        case ret: AsyncRPCClient.ReturnInvocation =>
          val retV2 = ControlCommandConvertUtils.controlReturnToV2(ret.controlReturn)
          ReturnInvocationV2(ret.originalCommandID, retV2)
        case _ =>
          throw new RuntimeException(
            s"control payload ${control.controlPayload} is neither an invocation nor a return value"
          )
      }
      ControlDeterminant(payloadV2, control.from).toByteArray
    } catch {
      case t: Throwable =>
        throw t
    }
  }

}
