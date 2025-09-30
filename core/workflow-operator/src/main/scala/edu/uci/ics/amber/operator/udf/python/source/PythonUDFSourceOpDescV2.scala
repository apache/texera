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

package edu.uci.ics.amber.operator.udf.python.source

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithCode
import edu.uci.ics.amber.core.tuple.{Attribute, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.OutputPort

class PythonUDFSourceOpDescV2 extends SourceOperatorDescriptor {

  @JsonProperty(
    required = true,
    defaultValue = "# from pytexera import *\n" +
      "# class GenerateOperator(UDFSourceOperator):\n" +
      "# \n" +
      "#     @overrides\n" +
      "#     \n" +
      "#     def produce(self) -> Iterator[Union[TupleLike, TableLike, None]]:\n" +
      "#         yield\n"
  )
  @JsonSchemaTitle("Python script")
  @JsonPropertyDescription("Input your code here")
  var code: String = _

  @JsonProperty(required = true, defaultValue = "1")
  @JsonSchemaTitle("Worker count")
  @JsonPropertyDescription("Specify how many parallel workers to launch")
  var workers: Int = 1

  @JsonProperty()
  @JsonSchemaTitle("Columns")
  @JsonPropertyDescription("The columns of the source")
  var columns: List[Attribute] = List.empty

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    require(workers >= 1, "Need at least 1 worker.")
    val physicalOp = PhysicalOp
      .sourcePhysicalOp(workflowId, executionId, operatorIdentifier, OpExecWithCode(code, "python"))
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withIsOneToManyOp(true)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
      .withLocationPreference(Option.empty)

    if (workers > 1) {
      physicalOp
        .withParallelizable(true)
        .withSuggestedWorkerNum(workers)
    } else {
      physicalOp.withParallelizable(false)
    }
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "1-out Python UDF",
      "User-defined function operator in Python script",
      OperatorGroupConstants.PYTHON_GROUP,
      List.empty, // No input ports for a source operator
      List(OutputPort()),
      supportReconfiguration = true
    )
  }

  override def sourceSchema(): Schema = {
    if (columns != null && columns.nonEmpty) {
      Schema().add(columns)
    } else {
      Schema()
    }
  }
}
