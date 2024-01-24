package edu.uci.ics.amber.engine.architecture.common

import edu.uci.ics.amber.engine.architecture.common.ProcessingStepCursor.INIT_STEP
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity

object ProcessingStepCursor {
  // step value before processing any incoming message
  // processing first message will have step = 0
  val INIT_STEP: Long = -1L
}

class ProcessingStepCursor {
  private var currentStepCounter: Long = INIT_STEP
  private var currentChannel: ChannelIdentity = _

  def setCurrentChannel(ChannelIdentity: ChannelIdentity): Unit = {
    currentChannel = ChannelIdentity
  }

  def getStep: Long = currentStepCounter

  def getChannel: ChannelIdentity = currentChannel

  def stepIncrement(): Unit = {
    currentStepCounter += 1
  }

}
