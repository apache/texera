package edu.uci.ics.texera.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

class WorkflowCompiler(
    context: WorkflowContext
) extends LazyLogging {

  /**
    * Function to expand logical plan to physical plan
    * @return the expanded physical plan and a set of output ports that need storage
    */
  private def expandLogicalPlan(
      logicalPlan: LogicalPlan,
      logicalOpsToViewResult: List[String],
      errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]]
  ): (PhysicalPlan, Set[GlobalPortIdentity]) = {
    val terminalLogicalOps = logicalPlan.getTerminalOperatorIds
    val logicalOpsNeedingStorage =
      (terminalLogicalOps ++ logicalOpsToViewResult.map(OperatorIdentity(_))).toSet
    var physicalPlan = PhysicalPlan(operators = Set.empty, links = Set.empty)
    val outputPortsNeedingStorage: mutable.HashSet[GlobalPortIdentity] = mutable.HashSet()

    logicalPlan.getTopologicalOpIds.asScala.foreach(logicalOpId =>
      Try {
        val logicalOp = logicalPlan.getOperator(logicalOpId)

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

              // **Check for Python-based operator errors during code generation**
              if (physicalOp.isPythonBased) {
                val code = physicalOp.getCode
                val exceptionPattern = """#EXCEPTION DURING CODE GENERATION:\s*(.*)""".r

                exceptionPattern.findFirstMatchIn(code).foreach { matchResult =>
                  val errorMessage = matchResult.group(1).trim
                  val error =
                    new RuntimeException(s"Operator is not configured properly: $errorMessage")

                  errorList match {
                    case Some(list) => list.append((logicalOpId, error)) // Store error and continue
                    case None       => throw error // Throw immediately if no error list is provided
                  }
                }
              }
            }
          })

        // convert logical operators needing storage to output ports needing storage
        subPlan
          .topologicalIterator()
          .filter(opId => logicalOpsNeedingStorage.contains(opId.logicalOpId))
          .map(physicalPlan.getOperator)
          .foreach { physicalOp =>
            physicalOp.outputPorts
              .filterNot(_._1.internal)
              .foreach {
                case (outputPortId, _) =>
                  outputPortsNeedingStorage += GlobalPortIdentity(
                    opId = physicalOp.id,
                    portId = outputPortId
                  )
              }
          }
      } match {
        case Success(_) =>

        case Failure(err) =>
          errorList match {
            case Some(list) => list.append((logicalOpId, err))
            case None       => throw err
          }
      }
    )
    (physicalPlan, outputPortsNeedingStorage.toSet)
  }

  /**
    * Compile a workflow to physical plan, along with the schema propagation result and error(if any)
    *
    * Comparing to WorkflowCompilingService's compiler, which is used solely for workflow editing,
    *  This compile is used before executing the workflow.
    *
    * TODO: we should consider merge this compile with WorkflowCompilingService's compile
    * @param logicalPlanPojo the pojo parsed from workflow str provided by user
    * @return Workflow, containing the physical plan, logical plan and workflow context
    */
  def compile(
      logicalPlanPojo: LogicalPlanPojo
  ): Workflow = {
    // 1. convert the pojo to logical plan
    val logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)

    // 2. resolve the file name in each scan source operator
    logicalPlan.resolveScanSourceOpFileName(None)

    // 3. expand the logical plan to the physical plan, and get a set of output ports that need storage
    val (physicalPlan, outputPortsNeedingStorage) =
      expandLogicalPlan(logicalPlan, logicalPlanPojo.opsToViewResult, None)

    context.workflowSettings =
      WorkflowSettings(context.workflowSettings.dataTransferBatchSize, outputPortsNeedingStorage)

    Workflow(context, logicalPlan, physicalPlan)
  }
}
