package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowMessage.getInMemSize
import edu.uci.ics.amber.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.workflow.PortIdentity

import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable

/* The abstracted FIFO/exactly-once logic */
class AmberFIFOChannel(val channelId: ChannelIdentity) extends AmberLogging {

  override def actorId: ActorVirtualIdentity = channelId.toWorkerId

  private val ofoMap = new mutable.HashMap[Long, WorkflowFIFOMessage]
  private var current = 0L
  private var enabled = true
  private val fifoQueue = new mutable.ListBuffer[WorkflowFIFOMessage]
  private val holdCredit = new AtomicLong()
  private var portId: Option[PortIdentity] = None

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

  def getCurrentSeq: Long = current

  private def isDuplicated(sequenceNumber: Long): Boolean =
    sequenceNumber < current || ofoMap.contains(sequenceNumber)

  private def isAhead(sequenceNumber: Long): Boolean = sequenceNumber > current

  private def stash(sequenceNumber: Long, data: WorkflowFIFOMessage): Unit = {
    ofoMap(sequenceNumber) = data
  }

  private def enforceFIFO(data: WorkflowFIFOMessage): Unit = {
    fifoQueue.append(data)
    holdCredit.getAndAdd(getInMemSize(data))
    current += 1
    while (ofoMap.contains(current)) {
      val msg = ofoMap(current)
      fifoQueue.append(msg)
      holdCredit.getAndAdd(getInMemSize(msg))
      ofoMap.remove(current)
      current += 1
    }
  }

  def take: WorkflowFIFOMessage = {
    val msg = fifoQueue.remove(0)
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

  def getQueuedCredit: Long = {
    holdCredit.get()
  }

  def setPortId(portId: PortIdentity): Unit = {
    this.portId = Some(portId)
  }

  def getPortId: PortIdentity = {
    this.portId.get
  }
}
