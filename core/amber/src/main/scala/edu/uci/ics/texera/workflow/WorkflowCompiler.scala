package edu.uci.ics.texera.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.storage.result.{OpResultStorage, ResultStorage}
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.Utils.objectMapper
import edu.uci.ics.amber.operator.SpecialPhysicalOpFactory
import edu.uci.ics.amber.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.workflow.OutputPort.OutputMode.SINGLE_SNAPSHOT
import edu.uci.ics.amber.workflow.{PhysicalLink, PortIdentity}
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

class WorkflowCompiler(
    context: WorkflowContext
) extends LazyLogging {

  // function to expand logical plan to physical plan
  private def expandLogicalPlan(
      logicalPlan: LogicalPlan,
      logicalOpsToViewResult: List[String],
      errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]]
  ): PhysicalPlan = {
    val terminalLogicalOps = logicalPlan.getTerminalOperatorIds
    val toAddSink = (terminalLogicalOps ++ logicalOpsToViewResult).toSet
    var physicalPlan = PhysicalPlan(operators = Set.empty, links = Set.empty)
    // create a JSON object that holds pointers to the workflow's results in Mongo
    val resultsJSON = objectMapper.createObjectNode()
    val sinksPointers = objectMapper.createArrayNode()

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

        // assign the sinks to toAddSink operators' external output ports
        subPlan
          .topologicalIterator()
          .map(physicalPlan.getOperator)
          .flatMap { physicalOp =>
            physicalOp.outputPorts.map(outputPort => (physicalOp, outputPort))
          }
          .filter({
            case (physicalOp, (_, (outputPort, _, _))) =>
              toAddSink.contains(physicalOp.id.logicalOpId) && !outputPort.id.internal
          })
          .foreach({
            case (physicalOp, (_, (outputPort, _, schema))) =>
              val storage = ResultStorage.getOpResultStorage(context.workflowId)
              val storageKey = physicalOp.id.logicalOpId

              // due to the size limit of single document in mongoDB (16MB)
              // for sinks visualizing HTMLs which could possibly be large in size, we always use the memory storage.
              val storageType = {
                if (outputPort.mode == SINGLE_SNAPSHOT) OpResultStorage.MEMORY
                else OpResultStorage.defaultStorageMode
              }
              if (!storage.contains(storageKey)) {
                // get the schema for result storage in certain mode
                val sinkStorageSchema: Option[Schema] =
                  if (storageType == OpResultStorage.MONGODB) {
                    // use the output schema on the first output port as the schema for storage
                    Some(schema.right.get)
                  } else {
                    None
                  }
                storage.create(
                  s"${context.executionId}_",
                  storageKey,
                  storageType,
                  sinkStorageSchema
                )
                // add the sink collection name to the JSON array of sinks
                val storageNode = objectMapper.createObjectNode()
                storageNode.put("storageType", storageType)
                storageNode.put("storageKey", s"${context.executionId}_$storageKey")
                sinksPointers.add(storageNode)
              }

              val sinkPhysicalOp = SpecialPhysicalOpFactory.newSinkPhysicalOp(
                context.workflowId,
                context.executionId,
                storageKey.id,
                outputPort.mode
              )
              val sinkLink =
                PhysicalLink(
                  physicalOp.id,
                  outputPort.id,
                  sinkPhysicalOp.id,
                  PortIdentity(internal = true)
                )
              physicalPlan = physicalPlan.addOperator(sinkPhysicalOp).addLink(sinkLink)
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

    // update execution entry in MySQL to have pointers to the mongo collections
    resultsJSON.set("results", sinksPointers)
    ExecutionsMetadataPersistService.tryUpdateExistingExecution(context.executionId) {
      _.setResult(resultsJSON.toString)
    }
    physicalPlan
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

    // 3. Propagate the schema to get the input & output schemas for each port of each operator
    logicalPlan.propagateWorkflowSchema(context, None)

    // 4. expand the logical plan to the physical plan, and assign storage
    val physicalPlan = expandLogicalPlan(logicalPlan, logicalPlanPojo.opsToViewResult, None)

    Workflow(context, logicalPlan, physicalPlan)
  }
}
