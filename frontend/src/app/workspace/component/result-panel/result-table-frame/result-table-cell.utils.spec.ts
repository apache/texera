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

import { isImageDataUrl } from "./result-table-cell.utils";

describe("isImageDataUrl", () => {
  it("should recognize supported image data URLs", () => {
    expect(isImageDataUrl("data:image/png;base64,AAAA")).toBe(true);
    expect(isImageDataUrl("data:image/jpeg;base64,BBBB")).toBe(true);
    expect(isImageDataUrl("data:image/webp;base64,CCCC")).toBe(true);
  });

  it("should reject binary previews and non-image strings", () => {
    expect(isImageDataUrl("<binary 1010...001, size = 4 bytes>")).toBe(false);
    expect(isImageDataUrl("data:text/plain;base64,AAAA")).toBe(false);
    expect(isImageDataUrl(42)).toBe(false);
  });
});
