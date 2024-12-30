package edu.uci.ics.amber.compiler.model

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.FileResolver
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.WorkflowContext
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.operator.source.scan.ScanSourceOpDesc
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.core.workflow.PortIdentity
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.util.SupplierUtil

import java.util
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import scala.jdk.CollectionConverters.SetHasAsScala
import scala.util.{Failure, Success, Try}

object LogicalPlan {

  private def toJgraphtDAG(
      operatorList: List[LogicalOp],
      links: List[LogicalLink]
  ): DirectedAcyclicGraph[OperatorIdentity, LogicalLink] = {
    val workflowDag =
      new DirectedAcyclicGraph[OperatorIdentity, LogicalLink](
        null, // vertexSupplier
        SupplierUtil.createSupplier(classOf[LogicalLink]), // edgeSupplier
        false, // weighted
        true // allowMultipleEdges
      )
    operatorList.foreach(op => workflowDag.addVertex(op.operatorIdentifier))
    links.foreach(l =>
      workflowDag.addEdge(
        l.fromOpId,
        l.toOpId,
        l
      )
    )
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

  private lazy val jgraphtDag: DirectedAcyclicGraph[OperatorIdentity, LogicalLink] =
    LogicalPlan.toJgraphtDAG(operators, links)

  def getTopologicalOpIds: util.Iterator[OperatorIdentity] = jgraphtDag.iterator()

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
          scanOp.setFileUri(fileUri)
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
