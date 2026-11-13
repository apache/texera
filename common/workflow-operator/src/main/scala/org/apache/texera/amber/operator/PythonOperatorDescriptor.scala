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

package org.apache.texera.amber.operator

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  PhysicalOp,
  PhysicalPlan,
  PortIdentity,
  SchemaPropagationFunc
}

trait PythonOperatorDescriptor extends LogicalOp {

  /**
    * This descriptor's code with every loop variable its properties refer to read from the
    * iteration's state, and why each reference the code cannot read so is not bound
    * (`LoopVariableBinding.of`). It generates the code anew on every call: a compile calls it
    * once, and builds the physical operator from the binding with
    * `getPhysicalPlan(workflowId, executionId, binding)`.
    */
  def loopVariableBinding: LoopVariableBinding = LoopVariableBinding.of(this)

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = getPhysicalOp(workflowId, executionId, loopVariableBinding)

  // Final, so that the plan the compiler builds from a binding is the one this builds.
  final override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = getPhysicalPlan(workflowId, executionId, loopVariableBinding)

  /**
    * The physical plan that runs `binding`'s code, this descriptor's `loopVariableBinding`: the
    * compiler builds the binding once, and reports why references stay unbound from the same
    * binding. Final, like `getPhysicalOp` from a binding, so that the physical operator runs the
    * code the binding judged.
    */
  final def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      binding: LoopVariableBinding
  ): PhysicalPlan =
    PhysicalPlan(
      operators = Set(getPhysicalOp(workflowId, executionId, binding)),
      links = Set.empty
    )

  /**
    * The physical operator that runs `binding`'s code, this descriptor's `loopVariableBinding`,
    * unchanged, so that it runs the code the binding judged.
    */
  final def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      binding: LoopVariableBinding
  ): PhysicalOp = {
    val pythonCode = binding.code
    val physicalOp = if (asSource()) {
      PhysicalOp.sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(pythonCode, "python")
      )
    } else {
      PhysicalOp.oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(pythonCode, "python")
      )
    }

    physicalOp
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(parallelizable())
      .withPropagateSchema(SchemaPropagationFunc(binding.outputSchemas(_)))
  }

  def parallelizable(): Boolean = false

  def asSource(): Boolean = false

  /**
    * This method is to be implemented to generate the actual Python source code
    * based on operators predicates.
    *
    * @return a String representation of the executable Python source code.
    */
  def generatePythonCode(): String

  def getOutputSchemas(inputSchemas: Map[PortIdentity, Schema]): Map[PortIdentity, Schema]
}

object PythonOperatorDescriptor {

  /**
    * The call through which generated code reads loop variable `name` as text: pyamber's
    * `Operator.loop_variable_text` answers it from the iteration's state message.
    */
  def loopVariableLookup(name: String): String = s"self.loop_variable_text('$name')"

  /**
    * The call through which generated code reads loop variable `name` as a value of `kind`
    * (`integer` for an int property, `long`, `number` or `boolean`): pyamber's
    * `Operator.loop_variable_value` answers it from the iteration's state message, converting it
    * as the JVM's `StateReferenceBinding` does, and Jackson as it writes the value.
    */
  def loopVariableValueLookup(name: String, kind: String): String =
    s"self.loop_variable_value('$name', '$kind')"
}
