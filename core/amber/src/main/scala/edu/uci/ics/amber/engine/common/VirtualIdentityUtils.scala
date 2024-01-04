package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}

import scala.util.matching.Regex

object VirtualIdentityUtils {

  private val workerNamePattern: Regex = raw"Worker:WF(\w+)-(.+)-(\w+)-(\d+)".r

  def createWorkerIdentity(
      executionId: ExecutionIdentity,
      operator: String,
      layerName: String,
      workerId: Int
  ): ActorVirtualIdentity = {
    ActorVirtualIdentity(s"Worker:WF${executionId.id}-$operator-$layerName-$workerId")
  }

  def createWorkerIdentity(
      executionId: ExecutionIdentity,
      physicalOpId: PhysicalOpIdentity,
      workerId: Int
  ): ActorVirtualIdentity = {
    createWorkerIdentity(executionId, physicalOpId.logicalOpId.id, physicalOpId.layerName, workerId)
  }

  def getPhysicalOpId(workerId: ActorVirtualIdentity): PhysicalOpIdentity = {
    workerId.name match {
      case workerNamePattern(_, operator, layerName, _) =>
        PhysicalOpIdentity(OperatorIdentity(operator), layerName)
    }
  }

  def getWorkerIndex(workerId: ActorVirtualIdentity): Int = {
    workerId.name match {
      case workerNamePattern(_, _, _, idx) =>
        idx.toInt
    }
  }
}
