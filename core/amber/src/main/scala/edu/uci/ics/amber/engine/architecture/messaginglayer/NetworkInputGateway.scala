package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.logreplay.OrderEnforcer
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

import scala.collection.mutable

class NetworkInputGateway(val actorId: ActorVirtualIdentity)
    extends AmberLogging
    with Serializable
    with InputGateway {

  private val inputChannels =
    new mutable.HashMap[ChannelIdentity, AmberFIFOChannel]()

  private val ports: mutable.HashMap[PortIdentity, WorkerPort] = mutable.HashMap()

  private val enforcers = mutable.ListBuffer[OrderEnforcer]()

  def tryPickControlChannel: Option[AmberFIFOChannel] = {
    val ret = inputChannels
      .find {
        case (cid, channel) =>
          cid.isControl && channel.isEnabled && channel.hasMessage && enforcers.forall(enforcer =>
            enforcer.isCompleted || enforcer.canProceed(cid)
          )
      }
      .map(_._2)

    enforcers.filter(enforcer => enforcer.isCompleted).foreach(enforcer => enforcers -= enforcer)
    ret
  }

  def tryPickChannel: Option[AmberFIFOChannel] = {
    val control = tryPickControlChannel
    val ret = if (control.isDefined) {
      control
    } else {
      inputChannels
        .find({
          case (cid, channel) =>
            !cid.isControl && channel.isEnabled && channel.hasMessage && enforcers
              .forall(enforcer => enforcer.isCompleted || enforcer.canProceed(cid))
        })
        .map(_._2)
    }
    enforcers.filter(enforcer => enforcer.isCompleted).foreach(enforcer => enforcers -= enforcer)
    ret
  }

  def getAllDataChannels: Iterable[AmberFIFOChannel] =
    inputChannels.filter(!_._1.isControl).values

  // this function is called by both main thread(for getting credit)
  // and DP thread(for enqueuing messages) so a lock is required here
  def getChannel(channelId: ChannelIdentity): AmberFIFOChannel = {
    synchronized {
      inputChannels.getOrElseUpdate(channelId, new AmberFIFOChannel(channelId))
    }
  }

  def getAllControlChannels: Iterable[AmberFIFOChannel] =
    inputChannels.filter(_._1.isControl).values

  override def getAllChannels: Iterable[AmberFIFOChannel] = inputChannels.values

  override def addEnforcer(enforcer: OrderEnforcer): Unit = {
    enforcers += enforcer
  }

  override def getAllPorts(): Set[PortIdentity] = {
    this.ports.keys.toSet
  }

  def addPort(portId: PortIdentity): Unit = {
    // each port can only be added and initialized once.
    if (this.ports.contains(portId)) {
      return
    }
    this.ports(portId) = WorkerPort()
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
