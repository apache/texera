package edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy

import akka.actor.Address

trait DeployStrategy extends Serializable {

  def initialize(available: Array[Address]): Unit

  def next(): Address

}
