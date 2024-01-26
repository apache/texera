package edu.uci.ics.amber.engine.architecture.messaginglayer

import akka.actor.Cancellable
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.FlushNetworkBuffer
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation

import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

class WorkerTimerService(actorService: AkkaActorService) {

  private val enabledAdaptiveBatching = AmberConfig.enableAdaptiveNetworkBuffering
  private val adaptiveBatchInterval = AmberConfig.adaptiveBufferingTimeoutMs

  var adaptiveBatchingHandle: Option[Cancellable] = None
  var isPaused = false

  def startAdaptiveBatching(): Unit = {
    if (!enabledAdaptiveBatching) {
      return
    }
    if (this.adaptiveBatchingHandle.nonEmpty) {
      return
    }
    this.adaptiveBatchingHandle = Some(
      actorService.sendToSelfWithFixedDelay(
        0.milliseconds,
        FiniteDuration.apply(adaptiveBatchInterval, MILLISECONDS),
        ControlInvocation(
          AsyncRPCClient.IgnoreReplyAndDoNotLog,
          FlushNetworkBuffer()
        )
      )
    )
  }

  def stopAdaptiveBatching(): Unit = {
    if (adaptiveBatchingHandle.nonEmpty) {
      adaptiveBatchingHandle.get.cancel()
    }
    isPaused = false
  }

  def pauseAdaptiveBatching(): Unit = {
    stopAdaptiveBatching()
    isPaused = true
  }

  def resumeAdaptiveBatching(): Unit = {
    if (isPaused) {
      startAdaptiveBatching()
    }
  }
}
