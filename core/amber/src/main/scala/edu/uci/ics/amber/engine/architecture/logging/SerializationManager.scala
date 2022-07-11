package edu.uci.ics.amber.engine.architecture.logging

import com.twitter.chill.{Kryo, KryoInstantiator, KryoPool}
import edu.uci.ics.amber.engine.architecture.logging.SerializationManager.kryo
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter
import edu.uci.ics.amber.engine.common.ambermessage.{
  ControlInvocationV2,
  ControlPayload,
  ReturnInvocationV2
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

object SerializationManager {
  val POOL_SIZE = 10
  val kryo: KryoPool = KryoPool.withByteArrayOutputStream(POOL_SIZE, new KryoInstantiator)
}

class SerializationManager(determinantLogWriter: DeterminantLogWriter) {

  sealed trait SerializedDeterminant
  case class TimeStampDeterminant(timestamp: Long) extends SerializedDeterminant
  case class ControlDeterminant(controlPayload: ControlPayload, from: ActorVirtualIdentity)
      extends SerializedDeterminant
  case class DataOrderDeterminant(sender: ActorVirtualIdentity, count: Long)
      extends SerializedDeterminant

  def compressedWrite(allDeterminants: Iterable[InMemDeterminant]): Unit = {
    var sender: ActorVirtualIdentity = null
    var count = 0
    allDeterminants.foreach {
      case TimeStamp(value) =>
        determinantLogWriter.writeLogRecord(serialize(TimeStampDeterminant(value)))
      case pcm: ProcessControlMessage =>
        determinantLogWriter.writeLogRecord(
          serialize(ControlDeterminant(pcm.controlPayload, pcm.from))
        )
      case SenderChangeTo(actorVirtualIdentity) =>
        if (sender != null) {
          determinantLogWriter.writeLogRecord(serialize(DataOrderDeterminant(sender, count)))
        }
        sender = actorVirtualIdentity
        count = 1
      case ProcessDataTuple =>
        count += 1
    }
  }

  private def serialize(serializedDeterminant: SerializedDeterminant): Array[Byte] = {
    kryo.toBytesWithClass(serializedDeterminant)
  }

  private def deserialize(x: Array[Byte]): SerializedDeterminant = {
    kryo.fromBytes(x).asInstanceOf[SerializedDeterminant]
  }

}
