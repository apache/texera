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

import { serializePortIdentity } from "./port-identity-serde";
import { PortIdentity } from "../type/proto/org/apache/texera/amber/core/workflow";

describe("serializePortIdentity", () => {
  it("should serialize an external port as id_internal", () => {
    expect(serializePortIdentity({ id: 3, internal: false })).toBe("3_false");
  });

  it("should serialize an internal port", () => {
    expect(serializePortIdentity({ id: 0, internal: true })).toBe("0_true");
  });

  it("should serialize port id zero", () => {
    expect(serializePortIdentity({ id: 0, internal: false })).toBe("0_false");
  });

  it("should serialize a large multi-digit port id", () => {
    expect(serializePortIdentity({ id: 999999, internal: false })).toBe("999999_false");
  });

  it("should serialize a large multi-digit internal port id", () => {
    expect(serializePortIdentity({ id: 999999, internal: true })).toBe("999999_true");
  });

  it("should pass a negative port id through without validation", () => {
    expect(serializePortIdentity({ id: -1, internal: false })).toBe("-1_false");
  });

  it("should pass a negative internal port id through without validation", () => {
    expect(serializePortIdentity({ id: -1, internal: true })).toBe("-1_true");
  });

  it("should produce different keys for internal and external ports with the same id", () => {
    expect(serializePortIdentity({ id: 0, internal: true })).not.toBe(
      serializePortIdentity({ id: 0, internal: false })
    );
  });

  it("should produce different keys for different ids", () => {
    expect(serializePortIdentity({ id: 0, internal: false })).not.toBe(
      serializePortIdentity({ id: 1, internal: false })
    );
  });

  it("should be deterministic for equal inputs", () => {
    const first: PortIdentity = { id: 5, internal: true };
    const second: PortIdentity = { id: 5, internal: true };
    expect(serializePortIdentity(first)).toBe(serializePortIdentity(second));
  });

  it("should work as an object key for schema lookup", () => {
    const key = serializePortIdentity({ id: 1, internal: false });
    const schemaByPort: Record<string, string> = { [key]: "my-schema" };
    const lookupKey = serializePortIdentity({ id: 1, internal: false });
    expect(schemaByPort[lookupKey]).toBe("my-schema");
  });

  it("should stay parseable by the backend deserializer format", () => {
    expect(serializePortIdentity({ id: 3, internal: false }).split("_")).toEqual(["3", "false"]);
  });
});
