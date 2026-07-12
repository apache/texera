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

import { parseLogicalOperatorPortID } from "./logical-operator-port-serde";

describe("parseLogicalOperatorPortID", () => {
  it("should parse a valid input port ID", () => {
    expect(parseLogicalOperatorPortID("input-0")).toEqual({ portNumber: 0, portType: "input" });
  });

  it("should parse a valid output port ID with a multi-digit port number", () => {
    expect(parseLogicalOperatorPortID("output-12")).toEqual({ portNumber: 12, portType: "output" });
  });

  it("should return undefined for an unknown port type", () => {
    expect(parseLogicalOperatorPortID("foo-1")).toBeUndefined();
  });

  it("should return undefined when the port number is missing", () => {
    expect(parseLogicalOperatorPortID("input-")).toBeUndefined();
  });

  it("should return undefined for a malformed port ID with extra segments", () => {
    expect(parseLogicalOperatorPortID("input-1-2")).toBeUndefined();
  });

  it("should return undefined for an empty string", () => {
    expect(parseLogicalOperatorPortID("")).toBeUndefined();
  });
});
