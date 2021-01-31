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

class PauseManager(controlOutputPort: ControlOutputPort) {

  protected val logger: WorkflowLogger = WorkflowLogger("PauseManager")

  // current pause privilege level
  private val pausePrivilegeLevel = new AtomicInteger(PauseManager.NoPause)
  // yielded control of the dp thread
  // volatile is necessary otherwise main thread cannot notice the change.
  // volatile means read/writes are through memory rather than CPU cache
  @volatile private var dpThreadBlocker: CompletableFuture[Void] = _
  @volatile private var pausePromise: Promise[ExecutionPaused] = _

  /** pause functionality
    * both dp thread and actor can call this function
    * @param
    */
  def pause(): Future[ExecutionPaused] = {

    /*this line atomically applies the following logic:
      Level = Paused
      if(level >= pausePrivilegeLevel.get())
        pausePrivilegeLevel.set(level)
     */
    pausePromise = Promise[ExecutionPaused]()
    if (isPaused) {
      pausePromise.setValue(ExecutionPaused())
    }
    pausePrivilegeLevel.getAndUpdate { i =>
      if (PauseManager.Paused >= i) {
        PauseManager.Paused
      } else i
    }

    pausePromise
  }

  def isPaused: Boolean = isPauseSet() && dpThreadBlocker != null && !dpThreadBlocker.isDone

  /** resume functionality
    * only actor calls this function for now
    * @param
    */
  def resume(): Unit = {
    if (pausePrivilegeLevel.get() == PauseManager.NoPause) {
      logger.logInfo("already resumed")
      return
    }
    // only privilege level >= current pause privilege level can resume the worker
    pausePrivilegeLevel.set(PauseManager.NoPause)
    unblockDPThread()
  }

  /** check for pause in dp thread
    * only dp thread and operator logic can call this function
    * @throws
    */
  @throws[Exception]
  def checkForPause(): Unit = {
    // returns if not paused
    if (isPauseSet()) {
      blockDPThread()
    }
  }

  def isPauseSet(): Boolean = {
    (pausePrivilegeLevel.get() != PauseManager.NoPause)
  }

  /** block the thread by creating CompletableFuture and wait for completion
    */
  private[this] def blockDPThread(): Unit = {
    // create a future and wait for its completion
    this.dpThreadBlocker = new CompletableFuture[Void]
    // notify main actor thread
    if (pausePromise != null) {
      pausePromise.setValue(ExecutionPaused())
      pausePromise = null
    }
    // thread blocks here
    logger.logInfo(s"dp thread blocked")
    this.dpThreadBlocker.get
    logger.logInfo(s"dp thread resumed")
  }

  /** unblock DP thread by resolving the CompletableFuture
    */
  private[this] def unblockDPThread(): Unit = {
    // If dp thread suspended, release it
    if (dpThreadBlocker != null) {
      logger.logInfo("resume the worker by complete the future")
      this.dpThreadBlocker.complete(null)
    }
  }

}
