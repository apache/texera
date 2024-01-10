package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}

import scala.util.matching.Regex

object VirtualIdentityUtils {

  private val workerNamePattern: Regex = raw"Worker:WF(\d+)-E(\d+)-(.+)-(\w+)-(\d+)".r
  private val operatorUUIDPattern: Regex = raw"(\w+)-(.+)-(\w+)".r
  def createWorkerIdentity(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operator: String,
      layerName: String,
      workerId: Int
  ): ActorVirtualIdentity = {

    ActorVirtualIdentity(
      s"Worker:WF${workflowId.id}-E${executionId.id}-$operator-$layerName-$workerId"
    )
  }

  def createWorkerIdentity(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      physicalOpId: PhysicalOpIdentity,
      workerId: Int
  ): ActorVirtualIdentity = {
    createWorkerIdentity(
      workflowId,
      executionId,
      physicalOpId.logicalOpId.id,
      physicalOpId.layerName,
      workerId
    )
  }

  def getPhysicalOpId(workerId: ActorVirtualIdentity): PhysicalOpIdentity = {
    workerId.name match {
      case workerNamePattern(_, _, operator, layerName, _) =>
        PhysicalOpIdentity(OperatorIdentity(operator), layerName)
    }
  }

  def getWorkerIndex(workerId: ActorVirtualIdentity): Int = {
    workerId.name match {
      case workerNamePattern(_, _, _, _, idx) =>
        idx.toInt
    }
  }

  def toShorterString(workerId: ActorVirtualIdentity): String = {
    workerId.name match {
      case workerNamePattern(workflowId, executionId, operatorName, layerName, workerIndex) =>
        val shorterName = if (operatorName.length > 6) {
          operatorName match {
            case operatorUUIDPattern(op, _, postfix) => op + "-" + postfix.takeRight(6)
            case _                                   => operatorName.takeRight(6)
          }
        } else {
          operatorName
        }

        s"WF$workflowId-E$executionId-$shorterName-$layerName-$workerIndex"
      case _ => workerId.toString
    }
  }
}
