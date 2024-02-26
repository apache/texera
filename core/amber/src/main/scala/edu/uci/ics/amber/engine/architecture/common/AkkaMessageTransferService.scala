package edu.uci.ics.amber.engine.architecture.common

import akka.actor.Cancellable
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor.NetworkMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.{CongestionControl, FlowControl}
import edu.uci.ics.amber.engine.common.{AmberConfig, AmberLogging}
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}

import scala.collection.mutable
import scala.concurrent.duration.DurationInt

class AkkaMessageTransferService(
    actorService: AkkaActorService,
    refService: AkkaActorRefMappingService,
    handleBackpressure: Boolean => Unit
) extends AmberLogging {

  override def actorId: ActorVirtualIdentity = actorService.id

  var resendHandle: Cancellable = Cancellable.alreadyCancelled
  var creditPollingHandle: Cancellable = Cancellable.alreadyCancelled

  // add congestion control and flow control here
  val channelToCC = new mutable.HashMap[ChannelIdentity, CongestionControl]()
  val channelToFC = new mutable.HashMap[ChannelIdentity, FlowControl]()
  val messageIDToIdentity = new mutable.LongMap[ChannelIdentity]

  private var backpressured = false

  /** keeps track of every outgoing message.
    * Each message is identified by this monotonic increasing ID.
    * It's different from the sequence number and it will only
    * be used by the output gate.
    */
  private var networkMessageID = 0L

  def initialize(): Unit = {
    resendHandle = actorService.scheduleWithFixedDelay(30.seconds, 30.seconds, checkResend)
    val pollingInterval = AmberConfig.creditPollingIntervalInMs.millis
    creditPollingHandle =
      actorService.scheduleWithFixedDelay(pollingInterval, pollingInterval, checkCreditPolling)
  }

  def stop(): Unit = {
    resendHandle.cancel()
    creditPollingHandle.cancel()
  }

  private def checkCreditPolling(): Unit = {
    channelToFC.foreach {
      case (channel, fc) =>
        if (fc.isOverloaded) {
          refService.askForCredit(channel)
        }
    }
  }

  def send(msg: WorkflowFIFOMessage): Unit = {
    val networkMessage = NetworkMessage(networkMessageID, msg)
    messageIDToIdentity(networkMessageID) = msg.channelId
    networkMessageID += 1
    forwardToFlowControl(
      networkMessage,
      out => forwardToCongestionControl(out, refService.forwardToActor)
    )
  }

  private def forwardToFlowControl(
      msg: NetworkMessage,
      chainedStep: NetworkMessage => Unit
  ): Unit = {
    if (msg.internalMessage.channelId.isControl) {
      // skip flow control for all control channels
      chainedStep(msg)
    } else {
      val flowControl =
        channelToFC.getOrElseUpdate(msg.internalMessage.channelId, new FlowControl())
      flowControl.getMessagesToSend(msg).foreach { msg =>
        chainedStep(msg)
      }
      checkForBackPressure()
    }
  }

  private def forwardToCongestionControl(
      msg: NetworkMessage,
      chainedStep: NetworkMessage => Unit
  ): Unit = {
    val congestionControl =
      channelToCC.getOrElseUpdate(msg.internalMessage.channelId, new CongestionControl())
    if (congestionControl.canSend) {
      congestionControl.markMessageInTransit(msg)
      chainedStep(msg)
    } else {
      congestionControl.enqueueMessage(msg)
    }
  }

  def receiveAck(msgId: Long, ackedCredit: Long, queuedCredit: Long): Unit = {
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
    if (channelToFC.contains(channelId)) {
      channelToFC(channelId).decreaseInflightCredit(ackedCredit)
      updateChannelCreditFromReceiver(channelId, queuedCredit)
    }
  }

  def getAllUnAckedMessages: Iterable[WorkflowFIFOMessage] = {
    val fcMessages = channelToFC.values.flatMap { fc =>
      fc.getMessagesToSend.map(_.internalMessage)
    }
    val ccMessages = channelToCC.values.flatMap { cc =>
      cc.getAllMessages.map(_.internalMessage)
    }
    fcMessages ++ ccMessages
  }

  def updateChannelCreditFromReceiver(channelId: ChannelIdentity, queuedCredit: Long): Unit = {
    val flowControl = channelToFC.getOrElseUpdate(channelId, new FlowControl())
    flowControl.updateQueuedCredit(queuedCredit)
    flowControl.getMessagesToSend.foreach(out =>
      forwardToCongestionControl(out, refService.forwardToActor)
    )
    checkForBackPressure()
  }

  private def checkForBackPressure(): Unit = {
    val existOverloadedChannel = channelToFC.values.exists(_.isOverloaded)
    if (backpressured == existOverloadedChannel) {
      return
    }
    backpressured = existOverloadedChannel
    logger.debug(s"current backpressure status = $backpressured channel credits = ${channelToFC
      .map(c => c._1 -> c._2.getCredit)}")
    handleBackpressure(backpressured)
  }

  private def checkResend(): Unit = {
    refService.clearQueriedActorRefs()
    channelToCC.foreach {
      case (channel, cc) =>
        val msgsNeedResend = cc.getTimedOutInTransitMessages
        if (msgsNeedResend.nonEmpty) {
          logger.debug(s"output for $channel: ${cc.getStatusReport}")
        }
        if (refService.hasActorRef(channel.fromWorkerId)) {
          msgsNeedResend.foreach { msg =>
            refService.forwardToActor(msg)
          }
        }
    }
  }
}
