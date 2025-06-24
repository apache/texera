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

package edu.uci.ics.amber.compiler

import com.google.protobuf.timestamp.Timestamp
import com.typesafe.scalalogging.{LazyLogging, Logger}
import edu.uci.ics.amber.compiler.WorkflowCompiler.{
  collectOutputSchemaFromPhysicalPlan,
  convertErrorListToWorkflowFatalErrorMap
}
import edu.uci.ics.amber.compiler.model.{LogicalPlan, LogicalPlanPojo}
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalLink, PhysicalPlan, PortIdentity, WorkflowContext}
import edu.uci.ics.amber.core.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.core.workflowruntimestate.FatalErrorType.COMPILATION_ERROR
import edu.uci.ics.amber.core.workflowruntimestate.WorkflowFatalError

import java.time.Instant
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala

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
    physicalPlan: Option[PhysicalPlan], // if physical plan is none, the compilation is failed
    operatorIdToOutputSchemas: Map[OperatorIdentity, Map[PortIdentity, Option[Schema]]],
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

    logicalPlan.getTopologicalOpIds.asScala.foreach { logicalOpId =>
      val logicalOp = logicalPlan.getOperator(logicalOpId)
      val allUpstreamLinks = logicalPlan.getUpstreamLinks(logicalOp.operatorIdentifier)

      try {
        val subPlan = logicalOp.getPhysicalPlan(context.workflowId, context.executionId)

        subPlan
          .topologicalIterator()
          .map(subPlan.getOperator)
          .foreach { physicalOp =>
            val externalLinks = allUpstreamLinks
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
            physicalPlan = (externalLinks ++ internalLinks).foldLeft(physicalPlan) { (plan, link) =>
              plan.addLink(link)
            }

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
      } catch {
        case e: Throwable =>
          errorList match {
            case Some(list) => list.append((logicalOpId, e)) // Store error
            case None       => throw e // Throw if no list is provided
          }
      }
    }

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
    var opIdToOutputSchema: Map[OperatorIdentity, Map[PortIdentity, Option[Schema]]] = Map()
    // 1. convert the pojo to logical plan
    val logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)

    // 2. resolve the file name in each scan source operator
    logicalPlan.resolveScanSourceOpFileName(Some(errorList))

    // 3. expand the logical plan to the physical plan
    val physicalPlan = expandLogicalPlan(logicalPlan, Some(errorList))

    // 4. collect the output schema for each logical op
    // even if error is encountered when logical => physical, we still want to get the input schemas for rest no-error operators
    opIdToOutputSchema = collectOutputSchemaFromPhysicalPlan(physicalPlan, errorList)
    WorkflowCompilationResult(
      physicalPlan = if (errorList.nonEmpty) None else Some(physicalPlan),
      operatorIdToOutputSchemas = opIdToOutputSchema,
      // map each error from OpId to WorkflowFatalError, and report them via logger
      operatorIdToError = convertErrorListToWorkflowFatalErrorMap(logger, errorList.toList)
    )
  }
}
