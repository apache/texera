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

import { PortIdentity } from "../type/proto/edu/uci/ics/amber/engine/common/workflow";

/**
 * Serializes a PortIdentity object to a string in the format "{isInput}-{id}-{internal}"
 * @param portIdentity The PortIdentity object to serialize
 * @param isInput Whether the port is an input port
 * @returns A string representation of the PortIdentity (e.g., "input-1-true", "output-2-false")
 */
export function serializePortIdentity(portIdentity: PortIdentity, isInput: boolean): string {
  return `${isInput ? "input" : "output"}-${portIdentity.id}-${portIdentity.internal}`;
}

/**
 * Deserializes a string back to a PortIdentity object
 * @param serialized The serialized string in format "{isInput}-{id}-{internal}"
 * @returns A PortIdentity object and a boolean indicating whether the port is an input port
 * @throws Error if the string format is invalid
 */
export function deserializePortIdentity(serialized: string): { portIdentity: PortIdentity; isInput: boolean } {
  const parts = serialized.split("-");
  if (parts.length !== 3) {
    throw new Error(`Invalid serialized PortIdentity format: ${serialized}`);
  }

  const id = parseInt(parts[1], 10);
  if (isNaN(id)) {
    throw new Error(`Invalid id in serialized PortIdentity: ${parts[1]}`);
  }

  const internal = parts[2] === "true";
  if (parts[2] !== "true" && parts[2] !== "false") {
    throw new Error(`Invalid internal value in serialized PortIdentity: ${parts[2]}`);
  }

  return { portIdentity: { id, internal }, isInput: parts[0] === "input" };
}
