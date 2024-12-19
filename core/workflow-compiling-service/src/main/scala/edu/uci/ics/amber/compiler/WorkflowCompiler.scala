package edu.uci.ics.amber.compiler

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.{LazyLogging, Logger}
import edu.uci.ics.amber.compiler.WorkflowCompiler.{
  collectInputSchemaFromPhysicalPlan,
  convertErrorListToWorkflowFatalErrorMap
}

import edu.uci.ics.amber.compiler.model.{LogicalPlan, LogicalPlanPojo}
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.workflow.PhysicalLink
import edu.uci.ics.amber.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.amber.workflowruntimestate.WorkflowFatalError

import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

object WorkflowCompiler {
  // util function for extracting the error causes
  private def getStackTraceWithAllCauses(err: Throwable, topLevel: Boolean = true): String = {
    val header = if (topLevel) {
      "Stack trace for developers: \n\n"
    } else {
      "\n\nCaused by:\n"
    }
    val message = header + err.toString + "\n" + err.getStackTrace.mkString("\n")
    if (err.getCause != null) {
      message + getStackTraceWithAllCauses(err.getCause, topLevel = false)
    } else {
      message
    }
  }

  // util function for convert the error list to error map, and report the error in log
  private def convertErrorListToWorkflowFatalErrorMap(
      logger: Logger,
      errorList: List[(OperatorIdentity, Throwable)]
  ): Map[OperatorIdentity, WorkflowFatalError] = {
    val opIdToError = mutable.Map[OperatorIdentity, WorkflowFatalError]()
    errorList.map {
      case (opId, err) =>
        // map each error to WorkflowFatalError, and report them in the log
        logger.error(s"Error occurred in logical plan compilation for opId: $opId", err)
        opIdToError += (opId -> WorkflowFatalError(
          COMPILATION_ERROR,
          Timestamp(Instant.now),
          err.toString,
          getStackTraceWithAllCauses(err),
          opId.id
        ))
    }
    opIdToError.toMap
  }

  private def collectInputSchemaFromPhysicalPlan(
      physicalPlan: PhysicalPlan,
      errorList: ArrayBuffer[(OperatorIdentity, Throwable)] // Mandatory error list
  ): Map[OperatorIdentity, List[Option[Schema]]] = {
    val physicalInputSchemas =
      physicalPlan.operators.map { physicalOp =>
        // Process inputPorts and capture Throwable values in the errorList
        physicalOp.id -> physicalOp.inputPorts.values
          .filterNot(_._1.id.internal)
          .map {
            case (port, _, schema) =>
              schema match {
                case Left(err) =>
                  // Save the Throwable into the errorList
                  errorList.append((physicalOp.id.logicalOpId, err))
                  port.id -> None // Use None for this port
                case Right(validSchema) =>
                  port.id -> Some(validSchema) // Use the valid schema
              }
          }
          .toList // Convert to a list for further processing
      }

    // Group the physical input schemas by their logical operator ID and consolidate the schemas
    physicalInputSchemas
      .groupBy(_._1.logicalOpId)
      .view
      .mapValues(_.flatMap(_._2).toList.sortBy(_._1.id).map(_._2))
      .toMap
  }
}

case class WorkflowCompilationResult(
    physicalPlan: Option[PhysicalPlan], // if physical plan is none, the compilation is failed
    operatorIdToInputSchemas: Map[OperatorIdentity, List[Option[Schema]]],
    operatorIdToError: Map[OperatorIdentity, WorkflowFatalError]
)

class WorkflowCompiler(
    context: WorkflowContext
) extends LazyLogging {

  // function to expand logical plan to physical plan
  private def expandLogicalPlan(
      logicalPlan: LogicalPlan,
      errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]]
  ): PhysicalPlan = {
    var physicalPlan = PhysicalPlan(operators = Set.empty, links = Set.empty)

    logicalPlan.getTopologicalOpIds.asScala.foreach(logicalOpId =>
      Try {
        val logicalOp = logicalPlan.getOperator(logicalOpId)
        logicalOp.setContext(context)

        val subPlan = logicalOp.getPhysicalPlan(context.workflowId, context.executionId)
        subPlan
          .topologicalIterator()
          .map(subPlan.getOperator)
          .foreach({ physicalOp =>
            {
              val externalLinks = logicalPlan
                .getUpstreamLinks(logicalOp.operatorIdentifier)
                .filter(link => physicalOp.inputPorts.contains(link.toPortId))
                .flatMap { link =>
                  physicalPlan
                    .getPhysicalOpsOfLogicalOp(link.fromOpId)
                    .find(_.outputPorts.contains(link.fromPortId))
                    .map(fromOp =>
                      PhysicalLink(fromOp.id, link.fromPortId, physicalOp.id, link.toPortId)
                    )
                }

              val internalLinks = subPlan.getUpstreamPhysicalLinks(physicalOp.id)

              // Add the operator to the physical plan
              physicalPlan = physicalPlan.addOperator(physicalOp.propagateSchema())

              // Add all the links to the physical plan
              physicalPlan = (externalLinks ++ internalLinks)
                .foldLeft(physicalPlan) { (plan, link) => plan.addLink(link) }
            }
          })
      } match {
        case Success(_) =>
        case Failure(err) =>
          errorList match {
            case Some(list) => list.append((logicalOpId, err))
            case None       => throw err
          }
      }
    )
    physicalPlan
  }

  /**
    * Compile a workflow to physical plan, along with the schema propagation result and error(if any)
    *
    * @param logicalPlanPojo the pojo parsed from workflow str provided by user
    * @return WorkflowCompilationResult, containing the physical plan, input schemas per op and error per op
    */
  def compile(
      logicalPlanPojo: LogicalPlanPojo
  ): WorkflowCompilationResult = {
    val errorList = new ArrayBuffer[(OperatorIdentity, Throwable)]()
    var opIdToInputSchema: Map[OperatorIdentity, List[Option[Schema]]] = Map()
    // 1. convert the pojo to logical plan
    val logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)

    // - resolve the file name in each scan source operator
    logicalPlan.resolveScanSourceOpFileName(Some(errorList))

    // 3. expand the logical plan to the physical plan
    val physicalPlan = expandLogicalPlan(logicalPlan, Some(errorList))

    if (errorList.isEmpty) {
      // no error during the expansion, then do:
      // - collect the input schema for each op
      opIdToInputSchema = collectInputSchemaFromPhysicalPlan(physicalPlan, errorList)
    }

    WorkflowCompilationResult(
      physicalPlan = if (errorList.nonEmpty) None else Some(physicalPlan),
      operatorIdToInputSchemas = opIdToInputSchema,
      // map each error from OpId to WorkflowFatalError, and report them via logger
      operatorIdToError = convertErrorListToWorkflowFatalErrorMap(logger, errorList.toList)
    )
  }
}
