package edu.uci.ics.amber.engine.architecture.controller

class WorkerPortExecution extends Serializable {
  var completed: Boolean = false

  def setCompleted(): Unit = {
    completed = true
  }
}
