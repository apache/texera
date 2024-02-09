package edu.uci.ics.amber.engine.architecture.controller

class PortExecution extends Serializable {
  var completed: Boolean = false

  def setCompleted(): Unit = {
    completed = true
  }
}
