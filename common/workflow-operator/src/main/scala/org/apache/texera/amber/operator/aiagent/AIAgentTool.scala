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

package org.apache.texera.amber.operator.aiagent

import dev.langchain4j.agent.tool.ToolSpecification

/**
  * A single tool that the AI Agent can call inside its per-row execution loop.
  *
  * Mirrors the tool-factory shape used by the workflow-edit assistant in
  * agent-service: one object per tool, exposing a LangChain4j specification and
  * a synchronous executor that takes the model's argument JSON and returns a
  * string result. Errors should be returned via [[AIAgentToolResult.error]]
  * rather than thrown, so the model can read the failure and recover.
  */
trait AIAgentTool extends AutoCloseable with Serializable {
  def name: String
  def specification: ToolSpecification
  def execute(argumentsJson: String): String
  override def close(): Unit = {}
}

object AIAgentToolResult {
  final val ErrorPrefix = "[ERROR] "

  def ok(message: String): String = message
  def error(message: String): String = s"$ErrorPrefix$message"
  def isError(result: String): Boolean = result != null && result.startsWith(ErrorPrefix)
}
