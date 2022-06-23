package edu.uci.ics.amber.engine.architecture.worker

object PauseManager {
  final val NoPause = 0
  final val Paused = 1

  final case class ExecutionPaused()
}

class PauseManager {

  // current pause privilege level
  private var pausePrivilegeLevel = PauseManager.NoPause

  // used temporarily for reshape in sort.
  // TODO: Refactor to expose various pause levels
  var pausedByOperatorLogic = false

  /** pause functionality
    * both dp thread and actor can call this function
    * @param
    */
  def pause(): Unit = {

    /*this line atomically applies the following logic:
      Level = Paused
      if(level >= pausePrivilegeLevel.get())
        pausePrivilegeLevel.set(level)
     */
    pausePrivilegeLevel = PauseManager.Paused
  }

  def isPaused: Boolean = pausePrivilegeLevel == PauseManager.Paused

  /** resume functionality
    * only actor calls this function for now
    * @param
    */
  def resume(): Unit = {
    pausePrivilegeLevel = PauseManager.NoPause
  }

}
