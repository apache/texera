package edu.uci.ics.amber.compiler

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.compiler.model.{LogicalPlan, LogicalPlanPojo}
import edu.uci.ics.amber.compiler.util.SinkInjectionTransformer
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.virtualidentity.OperatorIdentity

import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class WorkflowCompilationResult(
                                      physicalPlan: Option[PhysicalPlan], // if physical plan is none, the compilation is failed
                                      operatorIdToInputSchemas: Map[OperatorIdentity, List[Option[Schema]]],
                                      operatorIdToError: Map[OperatorIdentity, WorkflowFatalError]
                                    )

class WorkflowCompiler(
                        context: WorkflowContext
                      ) extends LazyLogging {

  /**
   * Compile a workflow to physical plan, along with the schema propagation result and error(if any)
   *
   * @param logicalPlanPojo the pojo parsed from workflow str provided by user
   * @return WorkflowCompilationResult, containing the physical plan, input schemas per op and error per op
   */
  def compile(
               logicalPlanPojo: LogicalPlanPojo
             ): WorkflowCompilationResult = {
    // first compile the pojo to logical plan
    val errorList = new ArrayBuffer[(OperatorIdentity, Throwable)]()
    val opIdToError = mutable.Map[OperatorIdentity, WorkflowFatalError]()

    var logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)
    logicalPlan = SinkInjectionTransformer.transform(
      logicalPlanPojo.opsToViewResult,
      logicalPlan
    )
    logicalPlan.resolveScanSourceOpFileName(Some(errorList))
    logicalPlan.propagateWorkflowSchema(context, Some(errorList))
    // map compilation errors with op id
    if (errorList.nonEmpty) {
      errorList.foreach {
        case (opId, err) =>
          logger.error("error occurred in logical plan compilation", err)
          opIdToError += (opId -> WorkflowFatalError(
            COMPILATION_ERROR,
            Timestamp(Instant.now),
            err.toString,
            getStackTraceWithAllCauses(err),
            opId.id
          ))
      }
    }

    if (opIdToError.nonEmpty) {
      // encounter errors during compile pojo to logical plan,
      //   so directly return None as physical plan, schema map and non-empty error map
      return WorkflowCompilationResult(
        physicalPlan = None,
        operatorIdToInputSchemas = Map.empty,
        operatorIdToError = opIdToError.toMap
      )
    }
    // from logical plan to physical plan
    val physicalPlan = PhysicalPlan(context, logicalPlan)

    // Extract physical input schemas, excluding internal ports
    val physicalInputSchemas = physicalPlan.operators.map { physicalOp =>
      physicalOp.id -> physicalOp.inputPorts.values
        .filterNot(_._1.id.internal)
        .map {
          case (port, _, schema) => port.id -> schema.toOption
        }
    }

    // Group the physical input schemas by their logical operator ID and consolidate the schemas
    val opIdToInputSchemas = physicalInputSchemas
      .groupBy(_._1.logicalOpId)
      .view
      .mapValues(_.flatMap(_._2).toList.sortBy(_._1.id).map(_._2))
      .toMap

    WorkflowCompilationResult(Some(physicalPlan), opIdToInputSchemas, Map.empty)
  }
}