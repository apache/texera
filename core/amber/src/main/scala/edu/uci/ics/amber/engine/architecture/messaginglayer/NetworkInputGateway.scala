package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.logreplay.OrderEnforcer
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.ChannelID
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

class NetworkInputGateway(val actorId: ActorVirtualIdentity)
    extends AmberLogging
    with Serializable
    with InputGateway {

  private val inputChannels =
    new mutable.HashMap[ChannelID, AmberFIFOChannel]()

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
  def getChannel(channelId: ChannelID): AmberFIFOChannel = {
    synchronized {
      inputChannels.getOrElseUpdate(channelId, new AmberFIFOChannel(channelId))
    }
  }

  def getAllControlChannels: Iterable[AmberFIFOChannel] =
    inputChannels.filter(_._1.isControl).values

  override def addEnforcer(enforcer: OrderEnforcer): Unit = {
    enforcers += enforcer
  }
}
