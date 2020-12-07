package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import edu.uci.ics.amber.engine.architecture.worker.neo.PauseManager.{NoPause, Paused}

object PauseManager {
  final val NoPause = 0
  final val Paused = 1
}

class PauseManager {

  // current pause privilege level
  private val pausePrivilegeLevel = new AtomicInteger(PauseManager.NoPause)
  // yielded control of the dp thread
  private var currentFuture: CompletableFuture[Void] = _

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
    pausePrivilegeLevel.getAndUpdate(i => if (Paused >= i) Paused else i)
  }

  /** blocking wait for dp thread to pause
    * MUST be called in worker actor thread
    */
  def waitForDPThread(): Unit = {
    while (currentFuture == null) {
      //wait
    }
  }

  /** resume functionality
    * only actor calls this function for now
    * @param
    */
  def resume(): Unit = {
    if (pausePrivilegeLevel.get() == NoPause) {
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
    if (this.pausePrivilegeLevel.get() == PauseManager.NoPause) return
    blockDPThread()
  }

  /** block the thread by creating CompletableFuture and wait for completion
    */
  private[this] def blockDPThread(): Unit = {
    // create a future and wait for its completion
    this.currentFuture = new CompletableFuture[Void]
    // thread blocks here
    this.currentFuture.get
  }

  /** unblock DP thread by resolving the CompletableFuture
    */
  private[this] def unblockDPThread(): Unit = {
    // If dp thread suspended, release it
    if (this.currentFuture != null) {
      this.currentFuture.complete(null)
      this.currentFuture = null
    }
  }

}
