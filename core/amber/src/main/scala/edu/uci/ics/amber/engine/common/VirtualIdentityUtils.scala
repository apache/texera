package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity}

import scala.util.matching.Regex

object VirtualIdentityUtils {

  private val workerNamePattern: Regex = raw"Worker:WF(\w+)-(.+)-(\w+)-(\d+)".r

  def createWorkerIdentity(
      executionId: Long,
      operator: String,
      layer: String,
      workerId: Int
  ): ActorVirtualIdentity = {
    ActorVirtualIdentity(s"Worker:WF$executionId-$operator-$layer-$workerId")
  }

  def createWorkerIdentity(
      executionId: Long,
      layer: LayerIdentity,
      workerId: Int
  ): ActorVirtualIdentity = {
    ActorVirtualIdentity(s"Worker:WF$executionId-${layer.operator}-${layer.layerID}-$workerId")
  }

  def getOperator(workerId: ActorVirtualIdentity): LayerIdentity = {
    workerId.name match {
      case workerNamePattern(_, operator, layer, _) =>
        LayerIdentity(operator, layer)
    }
  }

  def getWorkerIndex(workerId: ActorVirtualIdentity): Int = {
    workerId.name match {
      case workerNamePattern(_, _, _, idx) =>
        idx.toInt
    }
  }
}
