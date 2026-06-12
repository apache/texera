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

package org.apache.texera.amber.operator.keywordSearch

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.filter.FilterOpDesc
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class KeywordSearchOpDesc extends FilterOpDesc with StandaloneCodeGenerator {

  @JsonProperty(required = true)
  @JsonSchemaTitle("attribute")
  @JsonPropertyDescription("column to search keyword on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("keywords")
  @JsonPropertyDescription("keywords")
  @JsonSchemaInject(json = """{"minLength": 1}""")
  var keyword: String = _

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
          "org.apache.texera.amber.operator.keywordSearch.KeywordSearchOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Keyword Search",
      operatorDescription = "Search for keyword(s) in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def generateStandaloneCode(): String = {
    // JVM uses Lucene MemoryIndex + StandardAnalyzer + QueryParser per row.
    // Best-effort approximation: split keyword on whitespace, treat as
    // OR-of-terms with word boundaries, case-insensitive. This does NOT honor
    // Lucene query syntax (phrases "a b", booleans +/-, wildcards *, fuzzy ~).
    val raw = Option(keyword).getOrElse("")
    val terms = raw.trim.split("\\s+").filter(_.nonEmpty).toList
    if (terms.isEmpty) return "out1df = in1df"

    val regexSpecials = Set('.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\')
    val escaped = terms.map(_.flatMap(c => if (regexSpecials.contains(c)) s"\\$c" else c.toString))
    val pattern = escaped.mkString("\\b(?:", "|", ")\\b")
    val pyLiteral = "\"" + pattern.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

    s"""out1df = in1df[in1df["$attribute"].astype(str).str.contains($pyLiteral, regex=True, case=False, na=False)].reset_index(drop=True)"""
  }
}
