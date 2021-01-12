package edu.uci.ics.amber.engine.architecture.worker.neo

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.ActorRef
import com.twitter.util.Promise
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.architecture.worker.neo.PauseManager.{NoPause, Paused}
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.ExecutionPaused
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity
import edu.uci.ics.amber.engine.common.promise.{
  PromiseCompleted,
  PromiseContext,
  PromiseManager,
  ReturnPayload,
  WorkflowPromise
}

object PauseManager {
  final val NoPause = 0
  final val Paused = 1
}

class PauseManager(controlOutputPort: ControlOutputPort) {

  protected val logger: WorkflowLogger = WorkflowLogger("PauseManager")

  // current pause privilege level
  private val pausePrivilegeLevel = new AtomicInteger(PauseManager.NoPause)
  // yielded control of the dp thread
  // volatile is necessary otherwise main thread cannot notice the change.
  // volatile means read/writes are through memory rather than CPU cache
  @volatile private var dpThreadBlocker: CompletableFuture[Void] = _
  @volatile private var promiseContextFromActorThread: PromiseContext = _

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
    pausePrivilegeLevel.getAndUpdate { i =>
      if (Paused >= i) {
        Paused
      } else i
    }

  }

  def registerNotifyContext(promiseContext: PromiseContext): Unit = {
    if (isPaused) {
      controlOutputPort.sendTo(
        VirtualIdentity.Self,
        ReturnPayload(promiseContext, ExecutionPaused())
      )
    } else {
      promiseContextFromActorThread = promiseContext
    }
  }

  def isPaused: Boolean = isPauseSet() && dpThreadBlocker != null && !dpThreadBlocker.isDone

  /** resume functionality
    * only actor calls this function for now
    * @param
    */
  def resume(): Unit = {
    if (pausePrivilegeLevel.get() == NoPause) {
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
    if (promiseContextFromActorThread != null) {
      controlOutputPort.sendTo(
        VirtualIdentity.Self,
        ReturnPayload(promiseContextFromActorThread, ExecutionPaused())
      )
      promiseContextFromActorThread = null
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
