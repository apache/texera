/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.common.compiler

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.{LazyLogging, Logger}
import org.apache.texera.common.compiler.WorkflowCompiler.{
  collectOutputSchemaFromPhysicalPlan,
  convertErrorListToWorkflowFatalErrorMap
}
import org.apache.texera.common.compiler.model.{LogicalPlan, LogicalPlanPojo}
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalPlan,
  PortIdentity,
  WorkflowContext
}
import org.apache.texera.amber.core.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import org.apache.texera.amber.core.workflowruntimestate.WorkflowFatalError

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
    errorList.foreach {
      case (opId, err) =>
        // map each error to WorkflowFatalError, and report them in the log
        logger.error(s"Error occurred in logical plan compilation for opId: $opId", err)
        // keep only the first error per operator: it is the root cause (e.g. a file
        // resolution failure), while later stages re-fail on the same operator with
        // less specific messages (e.g. schema propagation seeing an unresolved file)
        if (!opIdToError.contains(opId)) {
          opIdToError += (opId -> WorkflowFatalError(
            COMPILATION_ERROR,
            Timestamp(Instant.now),
            err.toString,
            getStackTraceWithAllCauses(err),
            opId.id
          ))
        }
    }
    opIdToError.toMap
  }

  private def collectOutputSchemaFromPhysicalPlan(
      physicalPlan: PhysicalPlan,
      errorList: ArrayBuffer[(OperatorIdentity, Throwable)]
  ): Map[OperatorIdentity, Map[PortIdentity, Option[Schema]]] = {

    // Collect output schemas per physical operator
    val physicalOutputSchemas =
      physicalPlan.operators.map { physicalOp =>
        val portSchemas = physicalOp.outputPorts.values
          .filterNot(_._1.id.internal)
          .map {
            case (port, _, schema) =>
              schema match {
                case Left(err) =>
                  errorList.append((physicalOp.id.logicalOpId, err))
                  port.id -> None
                case Right(validSchema) =>
                  port.id -> Some(validSchema)
              }
          }
          .toMap
        physicalOp.id -> portSchemas
      }

    // Group by logical operator ID and merge port schemas
    physicalOutputSchemas
      .groupBy(_._1.logicalOpId)
      .view
      .mapValues { list =>
        list.flatMap(_._2).toMap
      }
      .toMap
  }

}

case class WorkflowCompilationResult(
    logicalPlan: LogicalPlan,
    physicalPlan: Option[PhysicalPlan], // if physical plan is none, the compilation is failed
    operatorIdToOutputSchemas: Map[OperatorIdentity, Map[PortIdentity, Option[Schema]]],
    operatorIdToError: Map[OperatorIdentity, WorkflowFatalError],
    outputPortsNeedingStorage: Set[GlobalPortIdentity]
)

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
    * Compile a workflow to physical plan, along with the schema propagation result and error(if any).
    *
    * @param logicalPlanPojo the pojo parsed from workflow str provided by user
    * @param errorHandling   Lenient (editing-time, collect all errors) or Strict (pre-execution, throw)
    * @return WorkflowCompilationResult, containing the logical plan, physical plan, output schemas per
    *         op, errors per op, and the output ports that need storage
    */
  def compile(
      logicalPlanPojo: LogicalPlanPojo,
      errorHandling: CompilationErrorHandling = CompilationErrorHandling.Lenient
  ): WorkflowCompilationResult = {
    // Lenient collects into a buffer; Strict passes None so the first error is thrown.
    val errorList: Option[ArrayBuffer[(OperatorIdentity, Throwable)]] =
      errorHandling match {
        case CompilationErrorHandling.Lenient =>
          Some(new ArrayBuffer[(OperatorIdentity, Throwable)]())
        case CompilationErrorHandling.Strict => None
      }

    // 1. convert the pojo to logical plan
    val logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)

    // 2. resolve the file name in each scan source operator
    logicalPlan.resolveScanSourceOpFileName(errorList)

    // 3. expand the logical plan to the physical plan, and get the output ports that need storage
    val (physicalPlan, outputPortsNeedingStorage) =
      expandLogicalPlan(logicalPlan, logicalPlanPojo.opsToViewResult, errorList)

    // 4. collect the output schema for each logical op
    // even if error is encountered when logical => physical, we still want to get the input schemas
    // for the rest of the no-error operators. In Strict mode there is no buffer (errors already thrown),
    // so schema errors go to a throwaway buffer and do not affect the result.
    val schemaErrorList = errorList.getOrElse(new ArrayBuffer[(OperatorIdentity, Throwable)]())
    val opIdToOutputSchema = collectOutputSchemaFromPhysicalPlan(physicalPlan, schemaErrorList)

    val hasErrors = errorList.exists(_.nonEmpty)
    WorkflowCompilationResult(
      logicalPlan = logicalPlan,
      physicalPlan = if (hasErrors) None else Some(physicalPlan),
      operatorIdToOutputSchemas = opIdToOutputSchema,
      // map each error from OpId to WorkflowFatalError, and report them via logger
      operatorIdToError = convertErrorListToWorkflowFatalErrorMap(
        logger,
        errorList.map(_.toList).getOrElse(List.empty)
      ),
      outputPortsNeedingStorage = outputPortsNeedingStorage
    )
  }
}
