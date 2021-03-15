package edu.uci.ics.amber.engine.architecture.worker

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import com.twitter.util.{Future, Promise}
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.architecture.worker.PauseManager.ExecutionPaused
import edu.uci.ics.amber.engine.common.WorkflowLogger

object PauseManager {
  final val NoPause = 0
  final val Paused = 1

  final case class ExecutionPaused()
}

class PauseManager {

  protected val logger: WorkflowLogger = WorkflowLogger("PauseManager")

  // current pause privilege level
  private var pausePrivilegeLevel = PauseManager.NoPause

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
