package edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy

import akka.actor.Address

object RandomDeployment {
  def apply() = new RandomDeployment()
}

class RandomDeployment extends DeployStrategy {
  var available: Array[Address] = _

  override def initialize(available: Array[Address]): Unit = {
    this.available = available
  }

  override def next(): Address = {
    available(util.Random.nextInt(available.length))
  }
}
