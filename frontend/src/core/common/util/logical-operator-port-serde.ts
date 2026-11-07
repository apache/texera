/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Extracts the port index from a port ID string.
 * @param portId Port ID like "input-0", "output-1", etc.
 * @returns undefined if the portId is invalid; port number and the type of the port will be returned
 */
export function parseLogicalOperatorPortID(
  portId: string
): { portNumber: number; portType: "input" | "output" } | undefined {
  const match = portId.match(/^(input|output)-(\d+)$/);
  if (!match) {
    return undefined;
  }

  const portType = match[1] as "input" | "output";
  const portNumber = parseInt(match[2]);

  return { portNumber, portType };
}
