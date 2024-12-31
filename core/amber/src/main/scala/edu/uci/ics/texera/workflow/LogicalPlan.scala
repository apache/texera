package edu.uci.ics.texera.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.FileResolver
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.source.scan.ScanSourceOpDesc
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.util.SupplierUtil

import java.util
import scala.collection.mutable.ArrayBuffer
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

  def getOperator(opId: OperatorIdentity): LogicalOp = operatorMap(opId)

  def getTerminalOperatorIds: List[OperatorIdentity] =
    operatorMap.keys
      .filter(op => jgraphtDag.outDegreeOf(op) == 0)
      .toList

  def getUpstreamLinks(opId: OperatorIdentity): List[LogicalLink] = {
    links.filter(l => l.toOpId == opId)
  }

  /**
    * Resolve all user-given filename for the scan source operators to URIs, and call op.setFileUri to set the URi
    *
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
            errorList match {
              case Some(errList) =>
                errList.append((operator.operatorIdentifier, err))
              case None =>
                // Throw the error if no errorList is provided
                throw err
            }
        }

      case _ => // Skip non-ScanSourceOpDesc operators
    }
  }
}
