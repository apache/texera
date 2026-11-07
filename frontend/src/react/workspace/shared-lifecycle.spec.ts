/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { describe, expect, it } from "vitest";
import { SharedModel } from "../../core/workspace/service/workflow-graph/model/shared-model";
describe("shared model lifecycle", () => {
  it("releases the websocket provider's process lifecycle handler even for disconnected documents", () => {
    const before = process.listenerCount("exit");
    const model = new SharedModel(7, undefined, undefined, { connect: false });
    expect(process.listenerCount("exit")).toBe(before + 1);
    model.destroy();
    expect(process.listenerCount("exit")).toBe(before);
    expect(() => model.destroy()).not.toThrow();
  });
});
