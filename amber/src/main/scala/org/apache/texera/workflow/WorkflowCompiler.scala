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
import org.apache.texera.amber.compiler.PhysicalPlanExpander
import org.apache.texera.amber.compiler.model.{LogicalPlan, LogicalPlanPojo}
import org.apache.texera.amber.core.virtualidentity.OperatorIdentity
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, WorkflowContext}
import org.apache.texera.amber.engine.architecture.controller.Workflow

/**
  * The in-JVM compiler used right before execution. It shares the logical-to-physical expansion with
  * the workflow-compiling-service's compiler (both call [[PhysicalPlanExpander]]); on top of that it
  * computes the output ports that need result storage and wraps the plan in a runtime [[Workflow]]
  * that still carries the logical plan. The compiling-service's compiler instead reports schemas and
  * errors for the editor.
  */
class WorkflowCompiler(
    context: WorkflowContext
) extends LazyLogging {

  /**
    * Compile a workflow to a runnable [[Workflow]] (physical plan + logical plan + context).
    *
    * @param logicalPlanPojo the pojo parsed from the workflow string provided by the user
    */
  def compile(
      logicalPlanPojo: LogicalPlanPojo
  ): Workflow = {
    // 1. convert the pojo to a logical plan
    val logicalPlan: LogicalPlan = LogicalPlan(logicalPlanPojo)

    // 2. resolve the file name in each scan source operator (throws on failure: no error list)
    logicalPlan.resolveScanSourceOpFileName(None)

    // 3. expand the logical plan into a physical plan (shared with the compiling-service compiler)
    val physicalPlan = PhysicalPlanExpander.expand(context, logicalPlan, None)

    // 4. mark the output ports of terminal / to-view operators as needing result storage
    val logicalOpsNeedingStorage =
      (logicalPlan.getTerminalOperatorIds ++ logicalPlanPojo.opsToViewResult.map(OperatorIdentity(_))).toSet
    val outputPortsNeedingStorage: Set[GlobalPortIdentity] = physicalPlan.operators
      .filter(physicalOp => logicalOpsNeedingStorage.contains(physicalOp.id.logicalOpId))
      .flatMap { physicalOp =>
        physicalOp.outputPorts.keys
          .filterNot(_.internal)
          .map(portId => GlobalPortIdentity(opId = physicalOp.id, portId = portId))
      }
      .toSet

    context.workflowSettings = context.workflowSettings.copy(
      outputPortsNeedingStorage = outputPortsNeedingStorage
    )

    Workflow(context, Some(logicalPlan), physicalPlan)
  }
}
