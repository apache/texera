package edu.uci.ics.amber.engine.architecture.controller

import akka.actor.Cancellable
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.MonitoringHandler.ControllerInitiateMonitoring
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.QueryWorkerStatisticsHandler.ControllerInitiateQueryStatistics
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.SkewDetectionHandler.ControllerInitiateSkewDetection
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation

import scala.concurrent.duration.{DurationInt, FiniteDuration, MILLISECONDS}

class ControllerTimerService(
    controllerConfig: ControllerConfig,
    akkaActorService: AkkaActorService
) {
  var statusUpdateAskHandle: Option[Cancellable] = None
  var monitoringHandle: Option[Cancellable] = None
  var skewDetectionHandle: Option[Cancellable] = None

  def enableStatusUpdate(): Unit = {
    if (controllerConfig.statusUpdateIntervalMs.nonEmpty && statusUpdateAskHandle.isEmpty) {
      statusUpdateAskHandle = Option(
        akkaActorService.sendToSelfWithFixedDelay(
          0.milliseconds,
          FiniteDuration.apply(controllerConfig.statusUpdateIntervalMs.get, MILLISECONDS),
          ControlInvocation(
            AsyncRPCClient.IgnoreReplyAndDoNotLog,
            ControllerInitiateQueryStatistics()
          )
        )
      )
    }
  }

  def enableMonitoring(): Unit = {
    if (
      Constants.monitoringEnabled && controllerConfig.monitoringIntervalMs.nonEmpty && monitoringHandle.isEmpty
    ) {
      monitoringHandle = Option(
        akkaActorService.sendToSelfWithFixedDelay(
          0.milliseconds,
          FiniteDuration.apply(controllerConfig.monitoringIntervalMs.get, MILLISECONDS),
          ControlInvocation(
            AsyncRPCClient.IgnoreReplyAndDoNotLog,
            ControllerInitiateMonitoring()
          )
        )
      )
    }
  }

  def enableSkewHandling(): Unit = {
    if (
      Constants.reshapeSkewHandlingEnabled && controllerConfig.skewDetectionIntervalMs.nonEmpty && skewDetectionHandle.isEmpty
    ) {
      skewDetectionHandle = Option(
        akkaActorService.sendToSelfWithFixedDelay(
          Constants.reshapeSkewDetectionInitialDelayInMs.milliseconds,
          FiniteDuration.apply(controllerConfig.skewDetectionIntervalMs.get, MILLISECONDS),
          ControlInvocation(
            AsyncRPCClient.IgnoreReplyAndDoNotLog,
            ControllerInitiateSkewDetection()
          )
        )
      )
    }
  }

  def disableStatusUpdate(): Unit = {
    if (statusUpdateAskHandle.nonEmpty) {
      statusUpdateAskHandle.get.cancel()
      statusUpdateAskHandle = Option.empty
    }
  }

  def disableMonitoring(): Unit = {
    if (monitoringHandle.nonEmpty) {
      monitoringHandle.get.cancel()
      monitoringHandle = Option.empty
    }
  }

  def disableSkewHandling(): Unit = {
    if (skewDetectionHandle.nonEmpty) {
      skewDetectionHandle.get.cancel()
      skewDetectionHandle = Option.empty
    }
  }
}
