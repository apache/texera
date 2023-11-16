package edu.uci.ics.amber.engine.architecture.controller

class LinkExecution(totalReceiversCount: Long) extends Serializable {
  private var currentCompletedCount = 0L

  def incrementCompletedReceiversCount(): Unit = currentCompletedCount += 1

  def isCompleted: Boolean = currentCompletedCount == totalReceiversCount
}
