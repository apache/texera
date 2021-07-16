package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

object OrderingEnforcer {
  def reorderMessage[V](
      seqMap: mutable.AnyRefMap[ActorVirtualIdentity, OrderingEnforcer[V]],
      sender: ActorVirtualIdentity,
      seq: Long,
      payload: V
  ): Option[Iterable[V]] = {
    val entry = seqMap.getOrElseUpdate(sender, new OrderingEnforcer[V]())
    if (entry.isDuplicated(seq)) {
      None
    } else if (entry.isAhead(seq)) {
      entry.stash(seq, payload)
      None
    } else {
      Some(entry.enforceFIFO(seq, payload))
    }
  }
}

/* The abstracted FIFO/exactly-once logic */
class OrderingEnforcer[T] {

  val ofoMap = new mutable.LongMap[T]
  var current = 0L

  def isDuplicated(sequenceNumber: Long): Boolean =
    sequenceNumber < current || ofoMap.contains(sequenceNumber)

  def isAhead(sequenceNumber: Long): Boolean = sequenceNumber > current

  def stash(sequenceNumber: Long, data: T): Unit = {
    ofoMap(sequenceNumber) = data
  }

  def enforceFIFO(sequenceNumber: Long, data: T): List[T] = {
    val res = mutable.ArrayBuffer[T](data)
    current += 1
    while (ofoMap.contains(current)) {
      res.append(ofoMap(current))
      ofoMap.remove(current)
      current += 1
    }
    res.toList
  }
}
