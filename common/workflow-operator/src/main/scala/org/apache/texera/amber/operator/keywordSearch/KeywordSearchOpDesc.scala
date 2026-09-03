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
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.util.JSONUtils.objectMapper

class KeywordSearchOpDesc extends FilterOpDesc with StandaloneCodeGenerator {

  @JsonProperty(required = true)
  @JsonSchemaTitle("attribute")
  @JsonPropertyDescription("column to search keyword on")
  @AutofillAttributeName
  var attribute: String = _

  // The value is a Lucene query, and its lexer needs double quotes in pairs: one on its
  // own opens a phrase that never closes, and `parse` throws before a row is read. The
  // pattern walks the value the way the lexer does, so a quote a backslash escapes stays
  // a character in a term. The other syntactic characters are left alone, since which
  // uses of them parse depends on what follows and a stricter pattern would reject the
  // phrase and range queries that work today. Anchored, because the form validates with
  // `test`, which searches.
  @JsonProperty(required = true)
  @JsonSchemaTitle("keywords")
  @JsonPropertyDescription("keywords")
  @JsonSchemaInject(
    json = """{"minLength": 1, "pattern": "^(?:[^\"\\\\]|\\\\.|\"(?:[^\"\\\\]|\\\\.)*\")*$"}"""
  )
  var keyword: String = _

  @JsonProperty(required = true, defaultValue = "false")
  @JsonSchemaTitle("Case Sensitive")
  @JsonPropertyDescription("Whether the keyword is case sensitive or not")
  var isCaseSensitive: Boolean = false

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
    // The engine runs a Lucene query per row; a script matches the terms
    // themselves, so a query that uses the syntax — a phrase, a boolean, a
    // wildcard, a fuzzy match — reads here as the words it is written with.
    val raw = Option(keyword).getOrElse("")
    val terms = raw.trim.split("\\s+").filter(_.nonEmpty).toList
    if (terms.isEmpty) return "out1df = in1df"

    val regexSpecials = Set('.', '^', '$', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '\\')
    val escaped = terms.map(_.flatMap(c => if (regexSpecials.contains(c)) s"\\$c" else c.toString))
    val pattern = escaped.mkString("\\b(?:", "|", ")\\b")
    val pyLiteral = pyStringLiteral(pattern)
    val attrLit = pyStringLiteral(attribute)

    // `astype(str)` turns an empty cell into the text "nan", which a term can match,
    // so the rows with nothing in the column are dropped before the match rather
    // than left to `na=False`, which by then has no null to see.
    s"""out1df = in1df[in1df[$attrLit].notna() & in1df[$attrLit].astype(str).str.contains($pyLiteral, regex=True, case=False, na=False)].reset_index(drop=True)"""
  }
}
