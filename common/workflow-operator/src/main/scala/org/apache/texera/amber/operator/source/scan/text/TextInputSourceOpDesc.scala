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

package org.apache.texera.amber.operator.source.scan.text

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.Schema
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.metadata.annotations.UIWidget
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

class TextInputSourceOpDesc
    extends SourceOperatorDescriptor
    with TextSourceOpDesc
    with StandaloneCodeGenerator {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Text")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  @JsonPropertyDescription("Enter the input text. By default, each line becomes one tuple.")
  var textInput: String = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    sourcePhysicalOp(
      workflowId,
      executionId,
      "org.apache.texera.amber.operator.source.scan.text.TextInputSourceOpExec"
    )

  override def sourceSchema(): Schema = Schema().add(attributeName, attributeType.getType)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Text Input",
      operatorDescription = "Source data from manually inputted text",
      OperatorGroupConstants.INPUT_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )

  override def generateStandaloneCode(): String = {
    val text = objectMapper.writeValueAsString(textInput)
    val colLit = pyStringLiteral(attributeName)
    val buf = scala.collection.mutable.ArrayBuffer[String]()

    buf += s"_text = $text"

    if (attributeType.isSingle) {
      val valueExpr =
        if (TextSourceOpDesc.isBinaryAttribute(attributeType)) """_text.encode("utf-8")"""
        else "_text"
      buf += s"""out1df = pd.DataFrame({$colLit: [$valueExpr]})"""
    } else {
      val castExpr = TextSourceOpDesc.lineCastExpr(attributeType, "l", "l")
      buf += """_lines = _text.replace("\r\n", "\n").replace("\r", "\n").split("\n")"""
      buf += """if _lines[-1] == "":"""
      buf += "    _lines.pop()"
      val linesExpr = TextSourceOpDesc.sliceLinesExpr("_lines", fileScanOffset, fileScanLimit)
      buf += s"""out1df = pd.DataFrame({$colLit: [$castExpr for l in $linesExpr]})"""
      buf ++= TextSourceOpDesc.parseTimestampColumn(attributeType, colLit)
    }

    buf.mkString("\n")
  }

  override def standaloneHelpers(): Seq[String] =
    TextSourceOpDesc.lineHelpers(attributeType)
}
