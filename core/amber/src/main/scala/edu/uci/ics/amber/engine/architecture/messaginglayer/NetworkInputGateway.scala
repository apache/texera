package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.ChannelID
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable

class NetworkInputGateway(val actorId: ActorVirtualIdentity)
    extends AmberLogging
    with Serializable {

  private val inputChannels =
    new mutable.HashMap[ChannelID, AmberFIFOChannel]()

  def tryPickControlChannel: Option[AmberFIFOChannel] = {
    inputChannels
      .find {
        case (cid, channel) => cid.isControl && channel.isEnabled && channel.hasMessage
      }
      .map(_._2)
  }

  def tryPickChannel: Option[AmberFIFOChannel] = {
    val control = tryPickControlChannel
    if (control.isDefined) {
      control
    } else {
      inputChannels.values.find(c => c.isEnabled && c.hasMessage)
    }
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

}
