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

export function createToolResult(message: string): string {
  return message;
}

export function createErrorResult(error: string): string {
  return `[ERROR] ${error}`;
}

function formatLinkDescription(sourceOperatorId: string, targetOperatorId: string): string {
  return `${sourceOperatorId} --> ${targetOperatorId}`;
}

export function formatAddOperatorResult(
  operatorId: string,
  numInputPorts: number,
  numOutputPorts: number,
  createdLinks?: { source: string; target: string }[],
  deletedLinks?: { source: string; target: string }[]
): string {
  let summary = `Added operator ${operatorId}, input ports: ${numInputPorts}, output ports: ${numOutputPorts}`;
  if (deletedLinks && deletedLinks.length > 0) {
    summary += `, deleted links: [${deletedLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  if (createdLinks && createdLinks.length > 0) {
    summary += `, created links: [${createdLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  return summary;
}

export function formatModifyOperatorResult(
  operatorId: string,
  createdLinks?: { source: string; target: string }[],
  deletedLinks?: { source: string; target: string }[]
): string {
  let summary = `Operator ${operatorId} modified`;
  if (deletedLinks && deletedLinks.length > 0) {
    summary += `, deleted links: [${deletedLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  if (createdLinks && createdLinks.length > 0) {
    summary += `, created links: [${createdLinks.map(l => formatLinkDescription(l.source, l.target)).join(", ")}]`;
  }
  return summary;
}

export function formatExecuteOperatorResult(operatorId: string): string {
  return `Executed operator ${operatorId}`;
}

export function formatOperatorError(operatorId: string, error: string): string {
  return `Error on operator ${operatorId}: ${error}`;
}
