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

import { selectionBounds, intersectsSelection, isSelectionGesture } from "./selection-geometry";

describe("canvas selection geometry", () => {
  it("normalizes reverse drags and includes intersecting elements", () => {
    const bounds = selectionBounds({ x: 300, y: 200 }, { x: 100, y: 50 });
    expect(bounds).toEqual({ x: 100, y: 50, width: 200, height: 150 });
    expect(intersectsSelection(bounds, { x: 90, y: 70, width: 30, height: 20 })).toBe(true);
    expect(intersectsSelection(bounds, { x: 400, y: 70, width: 30, height: 20 })).toBe(false);
    expect(intersectsSelection(selectionBounds({ x: 0, y: 0 }, { x: 0, y: 0 }), bounds)).toBe(false);
  });
  it("reserves secondary drag and Alt-primary drag, leaving normal pan alone", () => {
    expect(isSelectionGesture({ button: 2, altKey: false })).toBe(true);
    expect(isSelectionGesture({ button: 0, altKey: true })).toBe(true);
    expect(isSelectionGesture({ button: 0, altKey: false })).toBe(false);
    expect(isSelectionGesture({ button: 1, altKey: true })).toBe(false);
  });
});
