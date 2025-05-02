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

package edu.uci.ics.texera.web.model.websocket.request

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonSubTypes, JsonTypeInfo}
import edu.uci.ics.texera.web.model.websocket.request.python.{
  DebugCommandRequest,
  PythonExpressionEvaluateRequest
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(
  Array(
    new Type(value = classOf[EditingTimeCompilationRequest]),
    new Type(value = classOf[HeartBeatRequest]),
    new Type(value = classOf[ModifyLogicRequest]),
    new Type(value = classOf[ResultPaginationRequest]),
    new Type(value = classOf[RetryRequest]),
    new Type(value = classOf[SkipTupleRequest]),
    new Type(value = classOf[WorkflowExecuteRequest]),
    new Type(value = classOf[WorkflowKillRequest]),
    new Type(value = classOf[WorkflowPauseRequest]),
    new Type(value = classOf[WorkflowResumeRequest]),
    new Type(value = classOf[PythonExpressionEvaluateRequest]),
    new Type(value = classOf[DebugCommandRequest]),
    new Type(value = classOf[WorkflowCheckpointRequest])
  )
)
trait TexeraWebSocketRequest {}
