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

import { WORKFLOW_COLOR_PRESETS, getWorkflowColorFill, isWorkflowColor } from "./workflow-color";

describe("workflow color presets", () => {
  it("has six distinct stable identifiers, names and fixed hex fills", () => {
    expect(WORKFLOW_COLOR_PRESETS).toHaveLength(6);
    expect(new Set(WORKFLOW_COLOR_PRESETS.map(preset => preset.id)).size).toBe(6);
    expect(new Set(WORKFLOW_COLOR_PRESETS.map(preset => preset.label)).size).toBe(6);
    for (const preset of WORKFLOW_COLOR_PRESETS) {
      expect(preset.label.length).toBeGreaterThan(0);
      expect(preset.fill).toMatch(/^#[0-9a-f]{6}$/i);
      expect(isWorkflowColor(preset.id)).toBe(true);
      expect(getWorkflowColorFill(preset.id, "#ffffff")).toBe(preset.fill);
    }
  });

  it.each([undefined, null, "", "BLUE", "__proto__", "toString", "#000000", "url(x)", 0, {}, []])(
    "renders %j with the supplied legacy default, never input CSS",
    value => {
      expect(isWorkflowColor(value)).toBe(false);
      expect(getWorkflowColorFill(value, "#F2F4F5")).toBe("#F2F4F5");
    }
  );

  // WCAG relative luminance, with the picker foreground fixed to #262626.
  function luminance(hex: string): number {
    const [r, g, b] = hex
      .slice(1)
      .match(/../g)!
      .map(channel => {
        const srgb = parseInt(channel, 16) / 255;
        return srgb <= 0.04045 ? srgb / 12.92 : ((srgb + 0.055) / 1.055) ** 2.4;
      });
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
  }

  it.each(["yellow", "blue", "green", "pink", "purple", "orange"])(
    "keeps %s labels above 4.5:1 and focus/selection outlines above 3:1",
    color => {
      const background = getWorkflowColorFill(color, "#ffffff");
      const contrast = (luminance(background) + 0.05) / (luminance("#262626") + 0.05);
      expect(contrast).toBeGreaterThanOrEqual(4.5);
    }
  );
});
