package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.collection.mutable

class InputManager(val actorId: ActorVirtualIdentity) extends AmberLogging {

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
}
