package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.common.{AmberLogging, Constants}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/* The abstracted FIFO/exactly-once logic */
class AmberFIFOChannel(val actorId: ActorVirtualIdentity) extends AmberLogging {

  private val ofoMap = new mutable.HashMap[Long, WorkflowFIFOMessage]
  private var current = 0L
  private var enabled = true
  private val fifoQueue = new mutable.Queue[WorkflowFIFOMessage]
  private val holdCredit = new AtomicLong()

  def acceptMessage(msg: WorkflowFIFOMessage): Unit = {
    val seq = msg.sequenceNumber
    val payload = msg.payload
    if (isDuplicated(seq)) {
      logger.debug(
        s"received duplicated message $payload with seq = $seq while current seq = $current"
      )
    } else if (isAhead(seq)) {
      logger.debug(s"received ahead message $payload with seq = $seq while current seq = $current")
      stash(seq, msg)
    } else {
      enforceFIFO(msg)
    }
  }

  private def isDuplicated(sequenceNumber: Long): Boolean =
    sequenceNumber < current || ofoMap.contains(sequenceNumber)

  private def isAhead(sequenceNumber: Long): Boolean = sequenceNumber > current

  private def stash(sequenceNumber: Long, data: WorkflowFIFOMessage): Unit = {
    ofoMap(sequenceNumber) = data
  }

  private def enforceFIFO(data: WorkflowFIFOMessage): Unit = {
    fifoQueue.enqueue(data)
    holdCredit.getAndAdd(getInMemSize(data))
    current += 1
    while (ofoMap.contains(current)) {
      val msg = ofoMap(current)
      fifoQueue.enqueue(msg)
      holdCredit.getAndAdd(getInMemSize(msg))
      ofoMap.remove(current)
      current += 1
    }
  }

  def take: WorkflowFIFOMessage = {
    val msg = fifoQueue.dequeue()
    holdCredit.getAndAdd(-getInMemSize(msg))
    msg
  }

  def hasMessage: Boolean = fifoQueue.nonEmpty

  def enable(isEnabled: Boolean): Unit = {
    this.enabled = isEnabled
  }

  def isEnabled: Boolean = enabled

  def getTotalMessageSize: Long = {
    if (fifoQueue.nonEmpty) {
      fifoQueue.map(getInMemSize(_)).sum
    } else {
      0
    }
  }

  def getTotalStashedSize: Long =
    if (ofoMap.nonEmpty) {
      ofoMap.values.map(getInMemSize(_)).sum
    } else {
      0
    }

  def getAvailableCredits: Long = {
    Constants.unprocessedBatchesSizeLimitInBytesPerWorkerPair - holdCredit.get()
  }
}
