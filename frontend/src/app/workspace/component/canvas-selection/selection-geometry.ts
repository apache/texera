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

export interface SelectionBounds {
  x: number;
  y: number;
  width: number;
  height: number;
}
export function selectionBounds(a: { x: number; y: number }, b: { x: number; y: number }): SelectionBounds {
  return { x: Math.min(a.x, b.x), y: Math.min(a.y, b.y), width: Math.abs(a.x - b.x), height: Math.abs(a.y - b.y) };
}
export function intersectsSelection(a: SelectionBounds, b: SelectionBounds): boolean {
  return (
    a.width > 0 &&
    a.height > 0 &&
    a.x < b.x + b.width &&
    a.x + a.width > b.x &&
    a.y < b.y + b.height &&
    a.y + a.height > b.y
  );
}
export function isSelectionGesture(event: { button: number; altKey: boolean }): boolean {
  return event.button === 2 || (event.button === 0 && event.altKey);
}
