package edu.uci.ics.amber.util

import edu.uci.ics.amber.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}

import scala.util.matching.Regex

object VirtualIdentityUtils {

  private val workerNamePattern: Regex = raw"Worker:WF(\d+)-(.+)-(\w+)-(\d+)".r
  private val operatorUUIDPattern: Regex = raw"(\w+)-(.+)-(\w+)".r
  def createWorkerIdentity(
      workflowId: WorkflowIdentity,
      operator: String,
      layerName: String,
      workerId: Int
  ): ActorVirtualIdentity = {

    ActorVirtualIdentity(
      s"Worker:WF${workflowId.id}-$operator-$layerName-$workerId"
    )
  }

  def createWorkerIdentity(
      workflowId: WorkflowIdentity,
      physicalOpId: PhysicalOpIdentity,
      workerId: Int
  ): ActorVirtualIdentity = {
    createWorkerIdentity(
      workflowId,
      physicalOpId.logicalOpId.id,
      physicalOpId.layerName,
      workerId
    )
  }

  def getPhysicalOpId(workerId: ActorVirtualIdentity): PhysicalOpIdentity = {
    workerId.name match {
      case workerNamePattern(_, operator, layerName, _) =>
        PhysicalOpIdentity(OperatorIdentity(operator), layerName)
      case other =>
        // for special actorId such as SELF, CONTROLLER
        PhysicalOpIdentity(OperatorIdentity("__DummyOperator"), "__DummyLayer")
    }
  }

  def getWorkerIndex(workerId: ActorVirtualIdentity): Int = {
    workerId.name match {
      case workerNamePattern(_, _, _, idx) =>
        idx.toInt
    }
  }

  def toShorterString(workerId: ActorVirtualIdentity): String = {
    workerId.name match {
      case workerNamePattern(workflowId, operatorName, layerName, workerIndex) =>
        val shorterName = if (operatorName.length > 6) {
          operatorName match {
            case operatorUUIDPattern(op, _, postfix) => op + "-" + postfix.takeRight(6)
            case _                                   => operatorName.takeRight(6)
          }
        } else {
          operatorName
        }

        s"WF$workflowId-$shorterName-$layerName-$workerIndex"
      case _ => workerId.name
    }
  }
}
