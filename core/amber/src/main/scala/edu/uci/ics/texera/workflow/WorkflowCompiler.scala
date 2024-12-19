package edu.uci.ics.texera.workflow

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.storage.result.{OpResultStorage, ResultStorage}
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.PhysicalOp.getExternalPortSchemas
import edu.uci.ics.amber.core.workflow.{PhysicalOp, PhysicalPlan, SchemaPropagationFunc, WorkflowContext}
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.Utils.objectMapper
import edu.uci.ics.amber.operator.sink.ProgressiveUtils
import edu.uci.ics.amber.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.workflow.OutputPort.OutputMode.{SET_SNAPSHOT, SINGLE_SNAPSHOT}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort, PhysicalLink, PortIdentity}
import edu.uci.ics.amber.workflowruntimestate.WorkflowFatalError
import edu.uci.ics.texera.web.model.websocket.request.LogicalPlanPojo
import edu.uci.ics.texera.web.service.ExecutionsMetadataPersistService

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
  private def collectInputSchemasOfSinks(
      physicalPlan: PhysicalPlan,
      errorList: ArrayBuffer[(OperatorIdentity, Throwable)] // Mandatory error list
  ): Map[OperatorIdentity, Array[Schema]] = {
    physicalPlan.operators
      .filter(op => op.isSinkOperator)
      .map { physicalOp =>
        physicalOp.id.logicalOpId -> getExternalPortSchemas(
          physicalOp,
          fromInput = true,
          Some(errorList)
        ).flatten.toArray
      }
      .toMap
  }

  // Only collects the input schemas for the sink operator
  private def collectInputSchemaFromPhysicalPlanForSink(
      physicalPlan: PhysicalPlan,
      errorList: ArrayBuffer[(OperatorIdentity, Throwable)] // Mandatory error list
  ): Map[OperatorIdentity, List[Option[Schema]]] = {
    val physicalInputSchemas =
      physicalPlan.operators.filter(op => op.isSinkOperator).map { physicalOp =>
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

        // assign the view results
        subPlan.topologicalIterator().map(subPlan.getOperator).flatMap { physicalOp =>
          physicalOp.outputPorts.map(outputPort => (physicalOp, outputPort))
        }.filter({
          case (physicalOp, (_, (outputPort, _, _))) => toAddSink.contains(physicalOp.id.logicalOpId) && !outputPort.id.internal
        }).foreach({
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

            val sinkPhysicalOp = PhysicalOp.localPhysicalOp(
                context.workflowId,
                context.executionId,
                OperatorIdentity("sink_" + storageKey.id),
                OpExecInitInfo(
                  (idx, workers) =>
                    new edu.uci.ics.amber.operator.sink.managed.ProgressiveSinkOpExec(
                      outputPort.mode,
                      storageKey.id,
                      context.workflowId
                    )
                )
              )
              .withInputPorts(List(InputPort(PortIdentity())))
              .withOutputPorts(List(OutputPort(PortIdentity())))
              .withPropagateSchema(
                SchemaPropagationFunc((inputSchemas: Map[PortIdentity, Schema]) => {
                  // Get the first schema from inputSchemas
                  val inputSchema = inputSchemas.values.head

                  // Define outputSchema based on outputMode
                  val outputSchema = if (outputPort.mode == SET_SNAPSHOT) {
                    if (inputSchema.containsAttribute(ProgressiveUtils.insertRetractFlagAttr.getName)) {
                      // input is insert/retract delta: remove the flag column in the output
                      Schema.builder()
                        .add(inputSchema)
                        .remove(ProgressiveUtils.insertRetractFlagAttr.getName)
                        .build()
                    } else {
                      // input is insert-only delta: output schema is the same as input schema
                      inputSchema
                    }
                  } else {
                    // SET_DELTA: output schema is the same as input schema
                    inputSchema
                  }

                  // Create a Scala immutable Map
                  Map(PortIdentity() -> outputSchema)
                })
              )
            val sinkLink = PhysicalLink(physicalOp.id, outputPort.id, sinkPhysicalOp.id, PortIdentity())
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
    println("hello!!!" + resultsJSON.toString)
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
    var logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)

//    // 2. Manipulate logical plan by:
//    // - inject sink
//    logicalPlan = SinkInjectionTransformer.transform(
//      logicalPlanPojo.opsToViewResult,
//      logicalPlan
//    )
    // - resolve the file name in each scan source operator
    logicalPlan.resolveScanSourceOpFileName(None)

    // 3. Propagate the schema to get the input & output schemas for each port of each operator
    logicalPlan.propagateWorkflowSchema(context, None)

//    // 4. assign the sink storage using logical plan
//    assignSinkStorage(logicalPlan, context)

    // 5. expand the logical plan to the physical plan,
    val physicalPlan = expandLogicalPlan(logicalPlan, logicalPlanPojo.opsToViewResult, None)

    Workflow(context, logicalPlan, physicalPlan)
  }
}
