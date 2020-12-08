package edu.uci.ics.amber.engine.architecture.messaging

import edu.uci.ics.amber.engine.architecture.receivesemantics.FIFOAccessPort

class MessagingManager(val fifoEnforcer: FIFOAccessPort) {
  def receiveMessage(msg: Any): Unit = {}
}
