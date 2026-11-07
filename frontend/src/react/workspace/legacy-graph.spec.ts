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

import { afterAll, afterEach, vi } from "vitest";

const { cleanupProviders } = vi.hoisted(() => ({ cleanupProviders: new Set<() => void>() }));
vi.mock("y-websocket", async importOriginal => {
  const actual = await importOriginal<typeof import("y-websocket")>();
  return {
    ...actual,
    WebsocketProvider: class extends actual.WebsocketProvider {
      constructor(...args: ConstructorParameters<typeof actual.WebsocketProvider>) {
        super(...args);
        cleanupProviders.add(() => this.destroy());
      }
    },
  };
});
afterEach(() => {
  cleanupProviders.forEach(dispose => dispose());
  cleanupProviders.clear();
});

// These production models are framework independent. Execute their existing
// assertions unchanged in the React runner before adapting the UI around them.
class InertWebSocket {
  static CONNECTING = 0;
  static OPEN = 1;
  static CLOSING = 2;
  static CLOSED = 3;
  readyState = 3;
  close() {}
  send() {}
  addEventListener() {}
  removeEventListener() {}
}
vi.stubGlobal("WebSocket", InertWebSocket);
afterAll(() => vi.unstubAllGlobals());
await import("../../core/workspace/service/workflow-graph/model/workflow-graph.spec");
await import("../../core/workspace/service/workflow-graph/model/shared-model.spec");
