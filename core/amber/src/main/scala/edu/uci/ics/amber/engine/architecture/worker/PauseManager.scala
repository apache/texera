package edu.uci.ics.amber.engine.architecture.worker

import scala.collection.mutable

object PauseManager {
  final case class ExecutionPaused()
}

class PauseManager {

  private val pauseInvocations = new mutable.HashMap[PauseType.Value, Boolean]()

  def recordRequest(pauseType: PauseType.Value, enablePause: Boolean): Unit = {
    pauseInvocations(pauseType) = enablePause
  }

  def getPauseStatusByType(pauseType: PauseType.Value): Boolean =
    pauseInvocations.getOrElse(pauseType, false)

  def isPaused(): Boolean = {
    pauseInvocations.values.exists(isPaused => isPaused)
  }

}
