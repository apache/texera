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
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.StandaloneCodeGenerator
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.util.JSONUtils.objectMapper

class URLFetcherOpDesc extends SourceOperatorDescriptor with StandaloneCodeGenerator {

  // No `pattern`: the reader is `java.net.URL`, which asks only that the value carry
  // a scheme its JVM has a handler for. That is not something a regex can state --
  // one excluding `www.example.com` would still pass `htp://x`, so it would advertise
  // a validation the field does not have. `examples` offers a realistic value without
  // claiming to constrain anything.
  @JsonProperty(required = true)
  @JsonSchemaTitle("URL")
  @JsonPropertyDescription(
    "Only accepts standard URL format"
  )
  @JsonSchemaInject(json = """
{
  "examples": ["https://example.com"]
}
""")
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
        if (decodingMethod eq DecodingMethod.UTF_8) AttributeType.STRING else AttributeType.BINARY
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

  // The generated snippet uses `urllib.request`, which the translator's shared
  // imports don't include. Following the per-operator convention (e.g. Split
  // emits `import numpy as np`), the code block prepends its own import so the
  // generated script is self-contained.
  override def generateStandaloneCode(): String = {
    val urlLiteral = objectMapper.writeValueAsString(url)
    val isUtf8 = decodingMethod == DecodingMethod.UTF_8
    val valueExpr = if (isUtf8) """_content.decode("utf-8")""" else "_content"
    val buf = scala.collection.mutable.ArrayBuffer[String]()
    buf += "import urllib.request"
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
