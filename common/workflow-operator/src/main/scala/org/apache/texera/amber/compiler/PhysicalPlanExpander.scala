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

package org.apache.texera.amber.compiler

import org.apache.texera.amber.compiler.model.LogicalPlan
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{PhysicalLink, PhysicalPlan, WorkflowContext}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala

/**
  * The shared logical-to-physical expansion, used by both the in-JVM compiler (amber's
  * [[org.apache.texera.workflow.WorkflowCompiler]], which wraps it with storage planning and a
  * runtime Workflow) and the workflow-compiling-service's compiler (which wraps it with output
  * schema collection and error reporting). Keeping the expansion here avoids the two compilers
  * drifting apart again.
  */
object PhysicalPlanExpander {

  /**
    * Expand a [[LogicalPlan]] into a [[PhysicalPlan]]: for each logical operator (in topological
    * order) materialize its physical sub-plan, wire the external (cross-operator) and internal
    * links, and propagate schemas. Python operators whose code generation failed surface as errors.
    *
    * @param errorList if given, per-operator errors are appended and expansion continues; otherwise
    *                  the first error is thrown.
    */
  def expand(
      context: WorkflowContext,
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

            // Check for Python-based operator errors during code generation
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
        case err: Throwable =>
          errorList match {
            case Some(list) => list.append((logicalOpId, err))
            case None       => throw err
          }
      }
    }

    physicalPlan
  }
}
