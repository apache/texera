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

package org.apache.texera.amber.operator.source.http

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{OutputPort, PhysicalOp, SchemaPropagationFunc}
import org.apache.texera.amber.operator.http.util.{HttpMethod, KeyValuePair}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.util.JSONUtils.objectMapper

class PollingHttpSourceOpDesc extends SourceOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL")
  @JsonPropertyDescription("Endpoint to poll (e.g. https://api.example.com/feed)")
  var url: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("HTTP Method")
  @JsonPropertyDescription("HTTP method to use for each poll")
  var method: HttpMethod = HttpMethod.GET

  @JsonProperty(required = true)
  @JsonSchemaTitle("Interval (seconds)")
  @JsonPropertyDescription("Seconds to wait between polls")
  var intervalSeconds: Int = 30

  @JsonProperty(required = true)
  @JsonSchemaTitle("Max iterations")
  @JsonPropertyDescription("0 means poll forever; positive value caps the number of polls")
  var maxIterations: Int = 0

  @JsonProperty
  @JsonSchemaTitle("Headers")
  @JsonPropertyDescription("Optional headers, e.g. Authorization: Bearer <token>")
  var headers: java.util.List[KeyValuePair] = new java.util.ArrayList[KeyValuePair]()

  @JsonProperty
  @JsonSchemaTitle("Request Body")
  @JsonPropertyDescription("Body to send (POST/PUT/PATCH only); ignored for GET")
  var requestBody: String = ""

  override def sourceSchema(): Schema =
    Schema()
      .add("response_body", AttributeType.STRING)
      .add("status_code", AttributeType.INTEGER)
      .add("polled_at", AttributeType.TIMESTAMP)

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
          "org.apache.texera.amber.operator.source.http.PollingHttpSourceOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(false)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Polling HTTP Source",
      operatorDescription =
        "Repeatedly call an HTTP endpoint at a fixed interval and emit each response as a tuple",
      operatorGroupName = OperatorGroupConstants.API_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
}
