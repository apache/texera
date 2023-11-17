package edu.uci.ics.amber.engine.architecture.common

import akka.actor.Cancellable
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.{CongestionControl, FlowControl}
import edu.uci.ics.amber.engine.common.{AmberLogging, Constants}
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class AkkaMessageTransferService(
    actorService: AkkaActorService,
    refService: AkkaActorRefMappingService,
    handleBackpressure: Boolean => Unit
) extends AmberLogging {

  override def actorId: ActorVirtualIdentity = actorService.id

  var resendHandle: Cancellable = Cancellable.alreadyCancelled

  // add congestion control and flow control here
  val channelToCC = new mutable.HashMap[ChannelID, CongestionControl]()
  val channelToFC = new mutable.HashMap[ChannelID, FlowControl]()
  val messageIDToIdentity = new mutable.LongMap[ChannelID]

  private var backpressured = false

  /** keeps track of every outgoing message.
    * Each message is identified by this monotonic increasing ID.
    * It's different from the sequence number and it will only
    * be used by the output gate.
    */
  var networkMessageID = 0L

  def initialize(): Unit = {
    resendHandle = actorService.scheduleWithFixedDelay(30.seconds, 30.seconds, triggerResend)
  }

  def stop(): Unit = {
    resendHandle.cancel()
  }

  def send(msg: WorkflowFIFOMessage): Unit = {
    if (Constants.flowControlEnabled) {
      forwardToFlowControl(msg, out => forwardToCongestionControl(out, refService.forwardToActor))
    } else {
      forwardToCongestionControl(msg, refService.forwardToActor)
    }
  }

  private def forwardToFlowControl(
      msg: WorkflowFIFOMessage,
      chainedStep: WorkflowFIFOMessage => Unit
  ): Unit = {
    val flowControl = channelToFC.getOrElseUpdate(msg.channel, new FlowControl())
    flowControl.enqueueMessage(msg).foreach { msg =>
      chainedStep(msg)
    }
    checkForBackPressure()
  }

  private def forwardToCongestionControl(
      msg: WorkflowFIFOMessage,
      chainedStep: NetworkMessage => Unit
  ): Unit = {
    val congestionControl = channelToCC.getOrElseUpdate(msg.channel, new CongestionControl())
    val data = NetworkMessage(networkMessageID, msg)
    messageIDToIdentity(networkMessageID) = msg.channel
    if (congestionControl.canSend) {
      congestionControl.markMessageInTransit(data)
      chainedStep(data)
    } else {
      congestionControl.enqueueMessage(data)
    }
    networkMessageID += 1
  }

  def receiveAck(msgId: Long): Unit = {
    if (!messageIDToIdentity.contains(msgId)) {
      return
    }
    val channelId = messageIDToIdentity.remove(msgId).get
    val congestionControl = channelToCC.getOrElseUpdate(channelId, new CongestionControl())
    congestionControl.ack(msgId)
    congestionControl.getBufferedMessagesToSend.foreach { msg =>
      congestionControl.markMessageInTransit(msg)
      refService.forwardToActor(msg)
    }
  }

  def updateChannelCreditFromReceiver(channel: ChannelID, credit: Long): Unit = {
    val flowControl = channelToFC.getOrElseUpdate(channel, new FlowControl())
    flowControl.isPollingForCredit = false
    flowControl.updateCredit(credit)
    flowControl.getMessagesToSend.foreach(out =>
      forwardToCongestionControl(out, refService.forwardToActor)
    )
    checkForBackPressure()
  }

  private def checkForBackPressure(): Unit = {
    var existOverloadedChannel = false
    channelToFC.foreach {
      case (channel, fc) =>
        if (fc.isOverloaded) {
          existOverloadedChannel = true
          if (!fc.isPollingForCredit) {
            fc.isPollingForCredit = true
            actorService.scheduleOnce(
              Constants.creditPollingInitialDelayInMs.millis,
              () => {
                refService.askForCredit(channel)
              }
            )
          }
        }
    }
    if (backpressured == existOverloadedChannel) {
      return
    }
    backpressured = existOverloadedChannel
    logger.info(s"current backpressure status = $backpressured channel credits = ${channelToFC
      .map(c => c._1 -> c._2.senderSideCredit)}")
    handleBackpressure(backpressured)
  }

  private def triggerResend(): Unit = {
    refService.clearQueriedActorRefs()
    channelToCC.foreach {
      case (channel, cc) =>
        val msgsNeedResend = cc.getTimedOutInTransitMessages
        if (msgsNeedResend.nonEmpty) {
          logger.debug(s"output for $channel: ${cc.getStatusReport}")
        }
        if (refService.hasActorRef(channel.from)) {
          msgsNeedResend.foreach { msg =>
            refService.forwardToActor(msg)
          }
        }
    }
  }
}
