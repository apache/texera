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

package org.apache.texera.amber.operator.visualization.urlviz

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.{LogicalOp, StandaloneCodeGenerator}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

import javax.validation.constraints.NotNull

/**
  * URL Visualization operator to render any content in given URL link
  * This is the description of the operator
  */
@JsonSchemaInject(json = """
 {
   "attributeTypeRules": {
     "urlContentAttrName": {
       "enum": ["string"]
     }
   }
 }
 """)
class UrlVizOpDesc extends LogicalOp with StandaloneCodeGenerator {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL content")
  @AutofillAttributeName
  @NotNull(message = "URL content cannot be empty")
  val urlContentAttrName: String = ""

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .manyToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.visualization.urlviz.UrlVizOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => {
          val outputSchema = Schema().add("html-content", AttributeType.STRING)
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        })
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo.forVisualization(
      "URL Visualizer",
      "Render the content of URL",
      OperatorGroupConstants.VISUALIZATION_MEDIA_GROUP
    )

  // Output is a plain table (one "html-content" column), not a Plotly figure.
  override def producesDataFrame(): Boolean = true

  // Mirrors UrlVizOpExec: wrap each urlContentAttrName value in the exact same
  // iframe HTML document and emit it as the "html-content" column.
  override def generateStandaloneCode(): String =
    s"""def _texera_urlviz_iframe(u):
       |    return (
       |        '<!DOCTYPE html>\\n'
       |        '<html lang="en">\\n'
       |        '<body>\\n'
       |        '  <div class="modal-body">\\n'
       |        '    <iframe src="' + str(u) + '" frameborder="0"\\n'
       |        '       style="height:100vh; width:100%; border:none;">\\n'
       |        '    </iframe>\\n'
       |        '  </div>\\n'
       |        '</body>\\n'
       |        '</html>'
       |    )
       |out1df = pd.DataFrame({"html-content": in1df["$urlContentAttrName"].apply(_texera_urlviz_iframe)})""".stripMargin

}
