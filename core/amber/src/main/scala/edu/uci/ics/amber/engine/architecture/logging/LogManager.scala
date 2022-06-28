package edu.uci.ics.amber.engine.architecture.logging

import edu.uci.ics.amber.engine.architecture.logging.determinants.{ControlInput, DataInputOrder}
import edu.uci.ics.amber.engine.architecture.logging.storage.{
  DeterminantLogStorage,
  LocalFSLogStorage
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.SendRequest
import edu.uci.ics.amber.engine.architecture.worker.controlcommands.ControlCommandConvertUtils
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlInvocationV2,
  ControlPayload,
  ControlPayloadV2,
  ReturnInvocationV2
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class LogManager(
    networkCommunicationActor: NetworkCommunicationActor.NetworkSenderActorRef,
    actorId: ActorVirtualIdentity
) {

  private var cursor: Long = 0L
  private var prevCount: Long = 0L
  private var currentInput: ActorVirtualIdentity = _
  val enabledLogging: Boolean =
    AmberUtils.amberConfig.getBoolean("fault-tolerance.enable-determinant-logging")

  private val logStorage: DeterminantLogStorage = if (enabledLogging) {
    new LocalFSLogStorage(actorId.name.replace("Worker:", ""))
  } else {
    null
  }

  private val writer = if (enabledLogging) {
    val res = new AsyncLogWriter(networkCommunicationActor, logStorage)
    res.start()
    res
  } else {
    null
  }

  def logDataInputOrder(actorVirtualIdentity: ActorVirtualIdentity): Unit = {
    if (!enabledLogging) {
      return
    }
    if (currentInput == null || currentInput != actorVirtualIdentity) {
      writer.putDeterminant(DataInputOrder(prevCount, actorVirtualIdentity))
      prevCount = 0
      currentInput = actorVirtualIdentity
    }
    cursor += 1
    prevCount += 1
  }

  def logControlInput(command: ControlPayload, from: ActorVirtualIdentity): Unit = {
    if (!enabledLogging) {
      return
    }
    try {
      val payloadV2 = command match {
        case invocation: AsyncRPCClient.ControlInvocation =>
          val cmdV2 = ControlCommandConvertUtils.controlCommandToV2(invocation.command)
          ControlInvocationV2(invocation.commandID, cmdV2)
        case ret: AsyncRPCClient.ReturnInvocation =>
          val retV2 = ControlCommandConvertUtils.controlReturnToV2(ret.controlReturn)
          ReturnInvocationV2(ret.originalCommandID, retV2)
        case _ =>
          throw new RuntimeException(
            s"control payload $command is neither an invocation nor a return value"
          )
      }
      writer.putDeterminant(ControlInput(cursor, payloadV2, from))
      cursor += 1
    } catch {
      case throwable: Throwable => //skip
    }
  }

  def sendDirectlyOrCommitted(sendRequest: SendRequest): Unit = {
    if (!enabledLogging) {
      networkCommunicationActor ! sendRequest
    } else {
      writer.putOutput(sendRequest, cursor)
    }
  }

  def terminate(): Unit = {
    writer.terminate()
  }

}
