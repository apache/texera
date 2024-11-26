package edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy

import akka.actor.Address

object OneOnEach {
  def apply() = new OneOnEach()
}

class OneOnEach extends DeployStrategy {
  var available: Array[Address] = _
  var index = 0

  override def initialize(available: Array[Address]): Unit = {
    this.available = available
  }

  override def next(): Address = {
    val i = index
    if (i >= available.length) {
      throw new IndexOutOfBoundsException()
    }
    index += 1
    available(i)
  }
}
