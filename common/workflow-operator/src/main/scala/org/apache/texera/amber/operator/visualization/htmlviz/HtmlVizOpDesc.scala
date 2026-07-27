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

package org.apache.texera.amber.operator.visualization.htmlviz

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.{AutofillAttributeName, SampleColumn}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import javax.validation.constraints.NotNull

/**
  * HTML Visualization operator to render any given HTML code
  * This is the description of the operator
  */
class HtmlVizOpDesc extends LogicalOp with StandaloneCodeGenerator {
  @JsonProperty(required = true)
  @JsonSchemaTitle("HTML content")
  @AutofillAttributeName
  @SampleColumn("short_text")
  @NotNull(message = "HTML content cannot be empty")
  var htmlContentAttrName: String = ""

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.visualization.htmlviz.HtmlVizOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          val outputSchema = Schema().add("html-content", AttributeType.STRING)
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        })
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "HTML Visualizer",
      "Render the result of HTML content",
      OperatorGroupConstants.VISUALIZATION_MEDIA_GROUP
    )

  // Output is a plain table (one "html-content" column), not a Plotly figure.
  override def producesDataFrame(): Boolean = true

  // Mirrors HtmlVizOpExec: emit one row per input row whose single
  // "html-content" column is the value of htmlContentAttrName, passed through
  // unconverted (the exec does not coerce either).
  override def generateStandaloneCode(): String =
    s"""out1df = pd.DataFrame({"html-content": in1df["$htmlContentAttrName"]})"""

}
