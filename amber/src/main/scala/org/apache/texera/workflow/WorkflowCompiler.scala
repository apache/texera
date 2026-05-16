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

package org.apache.texera.workflow

import com.typesafe.scalalogging.LazyLogging
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.engine.architecture.controller.Workflow
import org.apache.texera.web.model.websocket.request.LogicalPlanPojo
import org.apache.texera.workflow.macroOp.{MacroExpander, MacroMappingCache, MacroRegistry}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.util.{Failure, Success, Try}

class WorkflowCompiler(
    context: WorkflowContext,
    macroRegistry: MacroRegistry = MacroRegistry.Empty
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
    val rawLogicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)

    // 2. expand any macro operators into a flat logical plan. Macros are a purely
    // logical-plan-level abstraction; after this pass the rest of the pipeline
    // never sees a MacroOpDesc / MacroInputOp / MacroOutputOp.
    val logicalPlan: LogicalPlan = MacroExpander.expand(rawLogicalPlan, macroRegistry)
    // Drain the macro-instance-provenance side-table populated by MacroExpander
    // and stash it in MacroMappingCache keyed by (wid, eid). The frontend
    // fetches this via GET /api/workflow/{wid}/macro-mapping?eid=... to roll
    // inner-op stats up to the macro op on the canvas (and to render stats
    // inside drill-down body views).
    val macroMapping = MacroExpander.takeMacroInstanceMapping()
    MacroMappingCache.put(context.workflowId, context.executionId, macroMapping)
    println(
      s"[MacroMappingCache] put wid=${context.workflowId.id} eid=${context.executionId.id} size=${macroMapping.size}"
    )
    // Debug: dump the post-expansion logical plan to a file so we can diff
    // it against a manually-flattened equivalent and confirm MacroExpander
    // produces a structurally identical plan. Keyed by workflow id so we
    // can correlate with execution logs.
    try {
      val wid = context.workflowId.id
      val opsDump = logicalPlan.operators.map(o =>
        Map(
          "type" -> o.getClass.getSimpleName,
          "id" -> o.operatorIdentifier.id,
          "inputPorts" -> Option(o.inputPorts).map(_.map(p =>
            Map(
              "portID" -> p.portID,
              "disallowMultiInputs" -> p.disallowMultiInputs,
              "isDynamicPort" -> p.isDynamicPort,
              "dependencies" -> p.dependencies
            )
          )).orNull,
          "outputPorts" -> Option(o.outputPorts).map(_.map(p =>
            Map("portID" -> p.portID, "disallowMultiInputs" -> p.disallowMultiInputs)
          )).orNull
        )
      )
      val linksDump = logicalPlan.links.map(l =>
        Map(
          "from" -> l.fromOpId.id,
          "fromPort" -> l.fromPortId.id,
          "to" -> l.toOpId.id,
          "toPort" -> l.toPortId.id
        )
      )
      val dump = Map("ops" -> opsDump, "links" -> linksDump)
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
        .registerModule(com.fasterxml.jackson.module.scala.DefaultScalaModule)
      val json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(dump)
      val outFile = new java.io.File(s"/tmp/texera-logs/expanded-plan-wid-$wid.json")
      java.nio.file.Files.writeString(outFile.toPath, json)
    } catch {
      case e: Throwable => // ignore debug-dump failures
    }

    // 3. resolve the file name in each scan source operator
    logicalPlan.resolveScanSourceOpFileName(None)

    // 4. expand the logical plan to the physical plan, and get a set of output ports that need storage
    val (physicalPlan, outputPortsNeedingStorage) =
      expandLogicalPlan(logicalPlan, logicalPlanPojo.opsToViewResult, None)

    context.workflowSettings = context.workflowSettings.copy(
      outputPortsNeedingStorage = outputPortsNeedingStorage
    )

    Workflow(context, logicalPlan, physicalPlan)
  }
}
