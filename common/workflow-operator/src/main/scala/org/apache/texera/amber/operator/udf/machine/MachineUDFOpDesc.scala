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

package org.apache.texera.amber.operator.udf.machine

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PhysicalOp.oneToOnePhysicalOp
import org.apache.texera.amber.core.workflow._
import org.apache.texera.amber.operator.map.MapOpDesc
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

/**
  * Machine UDF: like Python UDF, but the supplied Python code runs on a *registered machine*
  * via the machine-manager HTTP service rather than on a Texera computing unit. The tuple is
  * injected into the snippet as a global dict `tuple_in`, and the script is expected to print
  * a JSON object on its last stdout line; that JSON becomes the output tuple.
  */
class MachineUDFOpDesc extends MapOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Machine URL")
  @JsonPropertyDescription("Base URL of the target machine-manager, e.g. http://localhost:5555")
  var machineUrl: String = "http://localhost:5555"

  @JsonProperty
  @JsonSchemaTitle("Machine token")
  @JsonPropertyDescription("Bearer token for the machine-manager. Leave blank if not required.")
  var machineToken: String = ""

  @JsonProperty(
    required = true,
    defaultValue =
      "# `tuple_in` is the current input row as a dict.\n" +
        "# Print one JSON object on the last line to emit it as the output tuple.\n" +
        "import json\n" +
        "row = dict(tuple_in)\n" +
        "row['echoed'] = True\n" +
        "print(json.dumps(row))\n"
  )
  @JsonSchemaTitle("Python script")
  @JsonPropertyDescription(
    "Code executed on the target machine for each input tuple. The tuple is available as `tuple_in`."
  )
  var code: String = ""

  @JsonProperty(defaultValue = "60")
  @JsonSchemaTitle("Per-tuple timeout (seconds)")
  var timeoutSeconds: Int = 60

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Retain input columns")
  var retainInputColumns: Boolean = true

  @JsonProperty
  @JsonSchemaTitle("Extra output column(s)")
  @JsonPropertyDescription("Columns added by the script's returned JSON.")
  var outputColumns: List[Attribute] = List()

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    oneToOnePhysicalOp(
      workflowId,
      executionId,
      operatorIdentifier,
      OpExecWithClassName(
        "org.apache.texera.amber.operator.udf.machine.MachineUDFOpExec",
        objectMapper.writeValueAsString(this)
      )
    )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        val inputSchema = inputSchemas.values.head
        var outputSchema = if (retainInputColumns) inputSchema else Schema()
        if (outputColumns != null) {
          if (retainInputColumns) {
            for (column <- outputColumns) {
              if (inputSchema.containsAttribute(column.getName)) {
                throw new RuntimeException(
                  s"Column name ${column.getName} already exists on the input schema"
                )
              }
            }
          }
          outputSchema = outputSchema.add(outputColumns)
        }
        Map(operatorInfo.outputPorts.head.id -> outputSchema)
      }))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Machine UDF",
      "Run a Python snippet on a registered machine (via machine-manager) for each input tuple",
      OperatorGroupConstants.PYTHON_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}
