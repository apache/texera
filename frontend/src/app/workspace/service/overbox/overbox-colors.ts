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

export const OVERBOX_COLORS = [
  "#1677ff",
  "#722ed1",
  "#13a8a8",
  "#52c41a",
  "#fa8c16",
  "#eb2f96",
  "#f5222d",
  "#fadb14",
] as const;

const REPLACED_MUTED_COLORS = new Map<string, string>([
  ["#6f8fb7", "#1677ff"],
  ["#8877a5", "#722ed1"],
  ["#6d9995", "#13a8a8"],
  ["#7e9b77", "#52c41a"],
  ["#b18d69", "#fa8c16"],
  ["#a87c91", "#eb2f96"],
  ["#aa7474", "#f5222d"],
  ["#ad9b55", "#fadb14"],
]);

/** Keeps sections saved during the short-lived muted palette visually compatible. */
export function normalizeOverboxColor(color: string): string {
  const normalized = color.toLowerCase();
  return REPLACED_MUTED_COLORS.get(normalized) ?? (/^#[0-9a-f]{6}$/i.test(color) ? color : OVERBOX_COLORS[0]);
}
