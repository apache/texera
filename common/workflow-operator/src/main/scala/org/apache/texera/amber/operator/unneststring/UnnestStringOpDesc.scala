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

package org.apache.texera.amber.operator.unneststring

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalOp,
  SchemaPropagationFunc
}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.operator.flatmap.FlatMapOpDesc
import org.apache.texera.amber.operator.metadata.annotations.{AutofillAttributeName, SampleColumn}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class UnnestStringOpDesc extends FlatMapOpDesc with StandaloneCodeGenerator {
  @JsonProperty(value = "Delimiter", required = true, defaultValue = ",")
  @JsonPropertyDescription("string that separates the data")
  var delimiter: String = _

  @JsonProperty(value = "Attribute", required = true)
  @JsonPropertyDescription("column of the string to unnest")
  @AutofillAttributeName
  @SampleColumn("csv_list")
  var attribute: String = _

  @JsonProperty(value = "Result attribute", required = true, defaultValue = "unnestResult")
  @JsonPropertyDescription("column name of the unnest result")
  var resultAttribute: String = _

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Unnest String",
      operatorDescription =
        "Unnest the string values in the column separated by a delimiter to multiple values",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

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
          "org.apache.texera.amber.operator.unneststring.UnnestStringOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          val outputSchema = Option(resultAttribute)
            .filter(_.trim.nonEmpty)
            .map(attr => inputSchemas.values.head.add(attr, AttributeType.STRING))
            .getOrElse(throw new RuntimeException("Result attribute cannot be empty"))
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        })
      )
  }

  override def generateStandaloneCode(): String = {
    if (resultAttribute == null || resultAttribute.trim.isEmpty) {
      throw new RuntimeException("Result attribute cannot be empty")
    }
    // The JVM op uses Scala's `delimiter.r.split(...)`, so delimiter is a regex; it
    // and the two column names are rendered as escaped Python literals.
    val delim = pyStringLiteral(Option(delimiter).getOrElse(""))
    val resultLit = pyStringLiteral(resultAttribute)
    val attributeLit = pyStringLiteral(attribute)
    s"""# Nothing in the column unnests to nothing, the way the operator answers a null
       |# field with no rows at all. Dropped before the split rather than after:
       |# astype(str) would turn the empty cell into the text "None" and unnest that.
       |out1df = in1df[in1df[$attributeLit].notna()].copy()
       |out1df[$resultLit] = out1df[$attributeLit].astype(str).str.split($delim, regex=True)
       |out1df = out1df.explode($resultLit, ignore_index=True)
       |out1df = out1df[(out1df[$resultLit].notna()) & (out1df[$resultLit] != "")].reset_index(drop=True)""".stripMargin
  }
}
