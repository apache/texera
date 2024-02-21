package edu.uci.ics.amber.engine.architecture.controller.execution

case class WorkerPortExecution() {
  var completed: Boolean = false

  def setCompleted(): Unit = {
    completed = true
  }
}
