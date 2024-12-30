package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.core.tuple.{Schema, Tuple}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity

import scala.collection.mutable

class InputManager(val actorId: ActorVirtualIdentity) extends AmberLogging {
  private var inputBatch: Array[Tuple] = _
  private var currentInputIdx: Int = -1
  var currentChannelId: ChannelIdentity = _

  private val ports: mutable.HashMap[PortIdentity, WorkerPort] = mutable.HashMap()

  def getAllPorts: Set[PortIdentity] = {
    this.ports.keys.toSet
  }

  def addPort(portId: PortIdentity, schema: Schema): Unit = {
    // each port can only be added and initialized once.
    if (this.ports.contains(portId)) {
      return
    }
    this.ports(portId) = WorkerPort(schema)
  }

  def getPort(portId: PortIdentity): WorkerPort = ports(portId)

  def isPortCompleted(portId: PortIdentity): Boolean = {
    // a port without channels is not completed.
    if (this.ports(portId).channels.isEmpty) {
      return false
    }
    this.ports(portId).channels.values.forall(completed => completed)
  }

  def hasUnfinishedInput: Boolean = inputBatch != null && currentInputIdx + 1 < inputBatch.length

  def getNextTuple: Tuple = {
    currentInputIdx += 1
    inputBatch(currentInputIdx)
  }

  def getCurrentTuple: Tuple = {
    if (inputBatch == null) {
      null
    } else if (inputBatch.isEmpty) {
      null // TODO: create input exhausted
    } else {
      inputBatch(currentInputIdx)
    }
  }

  def initBatch(channelId: ChannelIdentity, batch: Array[Tuple]): Unit = {
    currentChannelId = channelId
    inputBatch = batch
    currentInputIdx = -1
  }
}
