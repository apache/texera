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

package org.apache.texera.amber.operator.http

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.http.util.{HttpMethod, KeyValuePair}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.util.JSONUtils.objectMapper

class HttpRequestOpDesc extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL")
  @JsonPropertyDescription(
    "Target URL. Supports ${fieldName} placeholders that will be filled from the input tuple."
  )
  var url: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("HTTP Method")
  @JsonPropertyDescription("HTTP method to use for each request")
  var method: HttpMethod = HttpMethod.POST

  @JsonProperty
  @JsonSchemaTitle("Headers")
  @JsonPropertyDescription("Optional headers, e.g. Authorization: Bearer <token>")
  var headers: java.util.List[KeyValuePair] = new java.util.ArrayList[KeyValuePair]()

  @JsonProperty
  @JsonSchemaTitle("Request Body Template")
  @JsonPropertyDescription(
    "Body sent with each request. Supports ${fieldName} placeholders from the input tuple. Ignored for GET."
  )
  var bodyTemplate: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Timeout (seconds)")
  @JsonPropertyDescription("Per-request timeout in seconds")
  var timeoutSeconds: Int = 10

  @JsonProperty(required = true)
  @JsonSchemaTitle("Fail on error")
  @JsonPropertyDescription(
    "If true, a non-2xx response or transport error fails the workflow. " +
      "If false (default), the error is recorded in the output tuple and processing continues."
  )
  var failOnError: Boolean = false

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
          "org.apache.texera.amber.operator.http.HttpRequestOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        val inputSchema = inputSchemas.values.head
        val outputSchema = inputSchema
          .add("http_request_status", AttributeType.INTEGER)
          .add("http_request_body", AttributeType.STRING)
          .add("http_request_error", AttributeType.STRING)
        Map(operatorInfo.outputPorts.head.id -> outputSchema)
      }))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "HTTP Request",
      operatorDescription =
        "For each input tuple, make an HTTP request (URL/body support ${fieldName} interpolation) and append the response.",
      operatorGroupName = OperatorGroupConstants.API_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}
