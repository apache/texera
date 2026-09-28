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

import { normalizeOverboxColor, OVERBOX_COLORS } from "./overbox-colors";

describe("overbox colors", () => {
  it("restores muted saved colors and retains the fixed original palette", () => {
    expect(normalizeOverboxColor("#6f8fb7")).toBe("#1677ff");
    expect(normalizeOverboxColor("#AD9B55")).toBe("#fadb14");
    expect(OVERBOX_COLORS).toContain("#fadb14");
  });
});
