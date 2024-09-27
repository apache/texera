package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.BroadcastMessageHandler.BroadcastMessage
import edu.uci.ics.amber.engine.architecture.logreplay.OrderEnforcer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.QueryStatisticsHandler.QueryStatistics
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StepHandler.{ContinueProcessing, StopProcessing}
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelMarkerPayload, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

import scala.collection.mutable

class NetworkInputGateway(val actorId: ActorVirtualIdentity)
    extends AmberLogging
    with Serializable
    with InputGateway {

  private val inputChannels =
    new mutable.HashMap[ChannelIdentity, AmberFIFOChannel]()

  @transient lazy private val enforcers = mutable.ListBuffer[OrderEnforcer]()

  def tryPickControlChannel: (Option[AmberFIFOChannel], Boolean) = {
    val ret = inputChannels
      .find {
        case (cid, channel) =>
          channel.peekMessage match {
            case Some(value) =>
              value match{
                case WorkflowFIFOMessage(_, _, ControlInvocation(_, BroadcastMessage(_, _))) =>
                  return (Some(channel),true)
                case WorkflowFIFOMessage(_, _, ControlInvocation(_, ContinueProcessing(_))) =>
                  return (Some(channel),true)
                case WorkflowFIFOMessage(_, _, ControlInvocation(_, StopProcessing())) =>
                  return (Some(channel),true)
                case WorkflowFIFOMessage(_, _, ControlInvocation(_, QueryStatistics())) =>
                  return (Some(channel), true)
                case other =>
                  //do nothing
              }
            case None => // do nothing
          }
          cid.isControl && channel.isEnabled && channel.hasMessage && enforcers.forall(enforcer =>
            enforcer.isCompleted || enforcer.canProceed(cid)
          )
      }
      .map(_._2)

    enforcers.filter(enforcer => enforcer.isCompleted).foreach(enforcer => enforcers -= enforcer)
    (ret, false)
  }

  def tryPickChannel: (Option[AmberFIFOChannel], Boolean) = {
    val (control, disableFT) = tryPickControlChannel
    val ret = if (control.isDefined) {
      control
    } else {
      inputChannels
        .find({
          case (cid, channel) =>
            channel.peekMessage match {
              case Some(value) =>
                value match {
                  case WorkflowFIFOMessage(_, _, ChannelMarkerPayload(id, _,_,_)) =>
                    if(id.id.contains("debugging")){
                      return (Some(channel), true)
                    }else{
                      // do nothing
                    }
                  case other =>
                  //do nothing
                }
              case None => // do nothing
            }
            !cid.isControl && channel.isEnabled && channel.hasMessage && enforcers
              .forall(enforcer => enforcer.isCompleted || enforcer.canProceed(cid))
        })
        .map(_._2)
    }
    enforcers.filter(enforcer => enforcer.isCompleted).foreach(enforcer => enforcers -= enforcer)
   (ret, disableFT)
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

}
