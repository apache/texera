package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.core.virtualidentity.ChannelIdentity

import scala.collection.mutable

class ReplayOrderEnforcer(
    logManager: ReplayLogManager,
    channelStepOrder: mutable.Queue[ProcessingStep],
    startStep: Long,
    private var onComplete: () => Unit
) extends OrderEnforcer {
  private var currentChannelId: ChannelIdentity = _

  private def triggerOnComplete(): Unit = {
    if (!isCompleted) {
      return
    }
    if (onComplete != null) {
      onComplete()
      onComplete = null // make sure the onComplete is called only once.
    }
  }

  // restore replay progress by dropping some of the entries
  while (channelStepOrder.nonEmpty && channelStepOrder.head.step <= startStep) {
    forwardNext()
  }

  var isCompleted: Boolean = channelStepOrder.isEmpty

  triggerOnComplete()

  private def forwardNext(): Unit = {
    if (channelStepOrder.nonEmpty) {
      val nextStep = channelStepOrder.dequeue()
      currentChannelId = nextStep.channelId
    }
  }

  def canProceed(channelId: ChannelIdentity): Boolean = {
    val step = logManager.getStep
    // release the next log record if the step matches
    // Note: To remove duplicate step orders caused by checkpoints
    // sending out a MainThreadDelegateMessage, we use a while loop.
    while (channelStepOrder.nonEmpty && channelStepOrder.head.step == step) {
      forwardNext()
    }
    // To terminate replay:
    // no next log record with step > current step, which means further processing is not logged.
    if (channelStepOrder.isEmpty) {
      isCompleted = true
      triggerOnComplete()
    }
    // only proceed if the current channel ID matches the channel ID of the log record
    currentChannelId == channelId
  }
}
