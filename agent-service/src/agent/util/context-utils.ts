/**
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

import type { ModelMessage } from "ai";

/**
 * A failed result for each of a step's tool calls that has none, or nothing when every call has
 * one. The SDK runs a step's calls only when the response ended normally, not when it was cut off
 * at the output limit or by a content filter, and it refuses a conversation in which a call has no
 * result. Kept as it is, such a step would fail every later turn.
 */
export function unexecutedToolCallsMessage(
  toolCalls: Array<{ toolCallId: string; toolName: string }>,
  resultsByCallId: ReadonlyMap<string, unknown>,
  finishReason: string
): ModelMessage | null {
  const unexecuted = toolCalls.filter(call => !resultsByCallId.has(call.toolCallId));
  if (unexecuted.length === 0) return null;
  return {
    role: "tool",
    content: unexecuted.map(call => ({
      type: "tool-result",
      toolCallId: call.toolCallId,
      toolName: call.toolName,
      output: {
        type: "error-text",
        value: `This call did not run: the response that made it ended with finish reason "${finishReason}".`,
      },
    })),
  };
}
