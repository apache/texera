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

import { afterAll, vi } from "vitest";

const originalGetContext = HTMLCanvasElement.prototype.getContext;
HTMLCanvasElement.prototype.getContext = (() => null) as typeof originalGetContext;
afterAll(() => {
  HTMLCanvasElement.prototype.getContext = originalGetContext;
});

// The original assertions exercise the extracted implementation directly.
// The temporary Angular injector wrapper is the only replaced collaborator.
vi.mock(
  "../../app/workspace/service/joint-ui/joint-ui.service",
  async () => await import("../../app/workspace/service/joint-ui/joint-ui")
);
await import("../../app/workspace/service/joint-ui/joint-ui.service.spec");
