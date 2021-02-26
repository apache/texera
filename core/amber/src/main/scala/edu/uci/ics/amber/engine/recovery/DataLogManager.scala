package edu.uci.ics.amber.engine.recovery

import com.google.common.collect.HashBiMap
import edu.uci.ics.amber.engine.common.ambermessage.DataPayload
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentity
import edu.uci.ics.amber.engine.recovery.DataLogManager.{
  DataLogElement,
  FromSender,
  IdentifierMapping
}

import scala.collection.mutable

object DataLogManager {
  sealed trait DataLogElement
  case class IdentifierMapping(virtualId: VirtualIdentity, id: Int) extends DataLogElement
  case class FromSender(id: Int) extends DataLogElement
}

class DataLogManager(logStorage: LogStorage[DataLogElement]) extends RecoveryComponent {

  private val idMappingForRecovery = HashBiMap.create[VirtualIdentity, Int]()
  private var counter = 0

  private val persistedDataOrder =
    logStorage
      .load()
      .flatMap {
        case msg: IdentifierMapping =>
          idMappingForRecovery.put(msg.virtualId, msg.id)
          None
        case msg: FromSender => Some(msg.id)
      }
      .to[mutable.Queue]

  private val stashedMessages = mutable.HashMap[VirtualIdentity, mutable.Queue[DataPayload]]()

  checkIfCompleted()

  def filterMessage(
      from: VirtualIdentity,
      message: DataPayload
  ): Iterable[(VirtualIdentity, DataPayload)] = {
    if (!isRecovering) {
      persistDataSender(from)
      return Iterable((from, message))
    }
    if (
      idMappingForRecovery
        .containsKey(from) && persistedDataOrder.head == idMappingForRecovery.get(from)
    ) {
      persistedDataOrder.dequeue()
      Iterable((from, message)) ++ checkStashedMessages()
    } else {
      if (stashedMessages.contains(from)) {
        stashedMessages(from).enqueue(message)
      } else {
        stashedMessages(from) = mutable.Queue[DataPayload](message)
      }
      Iterable.empty
    }
  }

  private[this] def persistDataSender(vid: VirtualIdentity): Unit = {
    if (!idMappingForRecovery.containsKey(vid)) {
      idMappingForRecovery.put(vid, counter)
      logStorage.persistElement(IdentifierMapping(vid, counter))
      counter += 1
    }
    logStorage.persistElement(FromSender(idMappingForRecovery.get(vid)))
  }

  private[this] def checkIfCompleted(): Unit = {
    if (persistedDataOrder.isEmpty && isRecovering) {
      setRecoveryCompleted()
    }
  }

  private[this] def checkStashedMessages(): Iterable[(VirtualIdentity, DataPayload)] = {
    val ret = mutable.ArrayBuffer[(VirtualIdentity, DataPayload)]()
    var cont = persistedDataOrder.nonEmpty
    while (cont) {
      val vid = idMappingForRecovery.inverse().get(persistedDataOrder.head)
      if (stashedMessages.contains(vid)) {
        val payload = stashedMessages(vid).dequeue()
        ret.append((vid, payload))
        persistedDataOrder.dequeue()
        cont = persistedDataOrder.nonEmpty
      } else {
        cont = false
      }
    }
    checkIfCompleted()
    ret
  }

}
