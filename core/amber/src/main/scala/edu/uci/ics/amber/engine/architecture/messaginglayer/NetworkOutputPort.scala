package edu.uci.ics.amber.engine.architecture.messaginglayer

import java.util.concurrent.atomic.AtomicLong

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SELF

import scala.collection.mutable

/**
  * NetworkOutput for generating sequence number when sending payloads
  * @param selfID ActorVirtualIdentity for the sender
  * @param handler actual sending logic
  * @tparam T payload
  */
class NetworkOutputPort[T](
    selfID: ActorVirtualIdentity,
    val handler: (ActorVirtualIdentity, ActorVirtualIdentity, Long, T) => Unit
) {
  private val idToSequenceNums = new mutable.AnyRefMap[ActorVirtualIdentity, AtomicLong]()
  def sendTo(to: ActorVirtualIdentity, payload: T): Unit = {
    var receiverId = to
    if (to == SELF) {
      // selfID and VirtualIdentity.SELF should be one key
      receiverId = selfID
    }
    val seqNum = idToSequenceNums.getOrElseUpdate(receiverId, new AtomicLong()).getAndIncrement()
    handler(to, selfID, seqNum, payload)
  }
}
