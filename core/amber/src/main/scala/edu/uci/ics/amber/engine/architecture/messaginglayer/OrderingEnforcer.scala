package edu.uci.ics.amber.engine.architecture.messaginglayer

import scala.collection.mutable

/* The abstracted FIFO/exactly-once logic */
class OrderingEnforcer[T] {

  val ofoMap = new mutable.LongMap[T]
  var current = 0L

  def setCurrent(value: Long): Unit = {
    current = value
  }

  def isDuplicated(sequenceNumber: Long): Boolean =
    sequenceNumber < current || ofoMap.contains(sequenceNumber)

  def isAhead(sequenceNumber: Long): Boolean = sequenceNumber > current

  def stash(sequenceNumber: Long, data: T): Unit = {
    ofoMap(sequenceNumber) = data
  }

  def enforceFIFO(data: T): List[T] = {
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
