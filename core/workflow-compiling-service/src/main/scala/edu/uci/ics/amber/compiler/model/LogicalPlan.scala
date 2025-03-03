package edu.uci.ics.amber.compiler.model

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.FileResolver
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.source.scan.ScanSourceOpDesc
import scalax.collection.mutable.Graph

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

object LogicalPlan {

  private def toScalaDAG(
      operatorList: List[LogicalOp],
      links: List[LogicalLink]
  ): Graph[OperatorIdentity, LogicalLink] = {
    val workflowDag = Graph.empty[OperatorIdentity, LogicalLink]()
    operatorList.foreach(op => workflowDag.add(op.operatorIdentifier))
    links.foreach(l => workflowDag.add(l))
    workflowDag
  }

  def apply(
      pojo: LogicalPlanPojo
  ): LogicalPlan = {
    LogicalPlan(pojo.operators, pojo.links)
  }

}

case class LogicalPlan(
    operators: List[LogicalOp],
    links: List[LogicalLink]
) extends LazyLogging {

  private lazy val operatorMap: Map[OperatorIdentity, LogicalOp] =
    operators.map(op => (op.operatorIdentifier, op)).toMap

  private lazy val scalaDAG: Graph[OperatorIdentity, LogicalLink] =
    LogicalPlan.toScalaDAG(operators, links)

  def getTopologicalOpIds: Iterator[OperatorIdentity] = {
    scalaDAG.topologicalSort match {
      case Left(value)  => throw new RuntimeException("topological sort failed.")
      case Right(value) => value.iterator.map(_.outer)
    }
  }

  def getOperator(opId: String): LogicalOp = operatorMap(OperatorIdentity(opId))

  def getOperator(opId: OperatorIdentity): LogicalOp = operatorMap(opId)

  def addOperator(op: LogicalOp): LogicalPlan = {
    // TODO: fix schema for the new operator
    this.copy(operators :+ op, links)
  }

  def addLink(
      fromOpId: OperatorIdentity,
      fromPortId: PortIdentity,
      toOpId: OperatorIdentity,
      toPortId: PortIdentity
  ): LogicalPlan = {
    val newLink = LogicalLink(
      fromOpId,
      fromPortId,
      toOpId,
      toPortId
    )
    val newLinks = links :+ newLink
    this.copy(operators, newLinks)
  }

  def getUpstreamLinks(opId: OperatorIdentity): List[LogicalLink] = {
    links.filter(l => l.toOpId == opId)
  }

  /**
    * Resolve all user-given filename for the scan source operators to URIs, and call op.setFileUri to set the URi
    * @param errorList if given, put errors during resolving to it
    */
  def resolveScanSourceOpFileName(
      errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]]
  ): Unit = {
    operators.foreach {
      case operator @ (scanOp: ScanSourceOpDesc) =>
        Try {
          // Resolve file path for ScanSourceOpDesc
          val fileName = scanOp.fileName.getOrElse(throw new RuntimeException("no input file name"))
          val fileUri = FileResolver.resolve(fileName) // Convert to URI

          // Set the URI in the ScanSourceOpDesc
          scanOp.setResolvedFileName(fileUri)
        } match {
          case Success(_) => // Successfully resolved and set the file URI
          case Failure(err) =>
            logger.error("Error resolving file path for ScanSourceOpDesc", err)
            errorList.foreach(_.append((operator.operatorIdentifier, err)))
        }
      case _ => // Skip non-ScanSourceOpDesc operators
    }
  }
}
