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
import org.apache.texera.amber.operator.http.util.KeyValuePair
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.SourceOperatorDescriptor
import org.apache.texera.amber.util.JSONUtils.objectMapper

class WebSocketSourceOpDesc extends SourceOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("WebSocket URL")
  @JsonPropertyDescription("ws:// or wss:// endpoint to subscribe to")
  var wsUrl: String = _

  @JsonProperty
  @JsonSchemaTitle("Subscribe Message")
  @JsonPropertyDescription(
    "Optional message sent to the server immediately after connecting (e.g. a JSON subscribe payload)"
  )
  var subscribeMessage: String = ""

  @JsonProperty
  @JsonSchemaTitle("Headers")
  @JsonPropertyDescription("Optional headers sent during the websocket handshake")
  var headers: java.util.List[KeyValuePair] = new java.util.ArrayList[KeyValuePair]()

  override def sourceSchema(): Schema =
    Schema()
      .add("message", AttributeType.STRING)
      .add("received_at", AttributeType.TIMESTAMP)

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
          "org.apache.texera.amber.operator.source.http.WebSocketSourceOpExec",
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
      userFriendlyName = "WebSocket Source",
      operatorDescription =
        "Connect to a websocket endpoint and emit each received frame as a tuple",
      operatorGroupName = OperatorGroupConstants.API_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
}
