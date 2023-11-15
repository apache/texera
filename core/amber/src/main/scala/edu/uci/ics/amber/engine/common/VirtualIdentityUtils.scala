package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity}

import scala.util.matching.Regex

object VirtualIdentityUtils {

  private val workerNamePattern: Regex = raw"Worker:WF(\w+)-(.+)-(\w+)-(\d+)".r

  def createWorkerIdentity(
      workflow: String,
      operator: String,
      layer: String,
      workerId: Int
  ): ActorVirtualIdentity = {
    ActorVirtualIdentity(s"Worker:WF$workflow-$operator-$layer-$workerId")
  }

  def getOperator(workerId: ActorVirtualIdentity): LayerIdentity = {
    workerId.name match {
      case workerNamePattern(workflow, operator, layer, _) =>
        LayerIdentity(workflow, operator, layer)
    }
  }

  def getWorkerIndex(workerId: ActorVirtualIdentity): Int = {
    workerId.name match {
      case workerNamePattern(_, _, _, idx) =>
        idx.toInt
    }
  }
}
