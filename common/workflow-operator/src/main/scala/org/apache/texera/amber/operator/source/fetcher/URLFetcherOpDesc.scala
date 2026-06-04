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

package org.apache.texera.amber.operator.source.fetcher

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.util.JSONUtils.objectMapper

class URLFetcherOpDesc extends SourceOperatorDescriptor with StandaloneCodeGenerator {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL")
  @JsonPropertyDescription(
    "Only accepts standard URL format"
  )
  var url: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Decoding")
  @JsonPropertyDescription(
    "The decoding method for the url content"
  )
  var decodingMethod: DecodingMethod = _

  override def sourceSchema(): Schema = {
    require(
      decodingMethod != null,
      "URLFetcherOpDesc.decodingMethod must be set before sourceSchema is computed"
    )
    Schema()
      .add(
        "URL content",
        if (decodingMethod == DecodingMethod.UTF_8) AttributeType.STRING else AttributeType.ANY
      )
  }

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "org.apache.texera.amber.operator.source.fetcher.URLFetcherOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "URL Fetcher",
      operatorDescription = "Fetch the content of a single URL",
      operatorGroupName = OperatorGroupConstants.API_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )

  // NOTE: the generated script uses `urllib.request.urlopen(...)`, but the
  // translator's shared imports don't include `urllib.request`. Per the rule
  // set 2026-05-26, we do not modify the translator on per-operator branches.
  // Users must prepend `import urllib.request` to the generated script before
  // running it. The manual test plan flags this. The import-management
  // strategy will be handled on the integration branch.
  override def generateStandaloneCode(): String = {
    val urlLiteral = objectMapper.writeValueAsString(url)
    val isUtf8 = decodingMethod == DecodingMethod.UTF_8
    val valueExpr = if (isUtf8) """_content.decode("utf-8")""" else "_content"
    val buf = scala.collection.mutable.ArrayBuffer[String]()
    buf += s"_url = $urlLiteral"
    buf += "try:"
    buf += "    with urllib.request.urlopen(_url) as _resp:"
    buf += "        _content = _resp.read()"
    buf += "except Exception:"
    buf += """    _content = f"Fetch failed for URL: {_url}".encode("utf-8")"""
    buf += s"""out1df = pd.DataFrame({"URL content": [$valueExpr]})"""
    buf.mkString("\n")
  }

}
