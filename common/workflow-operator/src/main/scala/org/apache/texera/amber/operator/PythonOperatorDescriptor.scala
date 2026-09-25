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

import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.state.StateReferencing.textReferences
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PortIdentity, SchemaPropagationFunc}
import org.apache.texera.amber.operator.PythonOperatorDescriptor.loopVariableLookup
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.decoderExpression
import org.apache.texera.amber.util.JSONUtils.objectMapper

trait PythonOperatorDescriptor extends LogicalOp {
  private def generatePythonCodeForRaisingException(ex: Throwable): String = {
    s"#EXCEPTION DURING CODE GENERATION: ${ex.getMessage}"
  }

  /**
    * `code` with every loop variable that one of this descriptor's String properties refers to
    * read from the iteration's state. The code is generated before the loop runs, from the
    * literal `$name` the property holds; pyb renders that text as a decode expression the
    * operator evaluates when it runs, which becomes `loopVariableLookup(name)` instead. A typed
    * placeholder (0 / 0.0 / false) is already a value in the code, so it stays (the compiler
    * rejects it), and outside every loop block the sidecar is empty and the code is untouched.
    */
  private def withLoopVariableLookups(code: String): String =
    if (stateReferences.isEmpty) {
      code
    } else {
      textReferences(objectMapper.valueToTree[ObjectNode](this), stateReferences).values.toSet
        .foldLeft(code) { (rewritten, name) =>
          rewritten.replace(decoderExpression("$" + name), loopVariableLookup(name))
        }
    }

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val pythonCode =
      try {
        withLoopVariableLookups(generatePythonCode())
      } catch {
        case ex: Throwable =>
          // instead of throwing error directly, we embed the error in the code
          // this can let upper-level compiler catch the error without interrupting the schema propagation
          generatePythonCodeForRaisingException(ex)
      }
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
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => getOutputSchemas(inputSchemas)))
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
}
