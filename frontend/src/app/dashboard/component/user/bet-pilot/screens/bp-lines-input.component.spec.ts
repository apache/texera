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

import { BpLinesInputComponent } from "./bp-lines-input.component";

describe("BpLinesInputComponent", () => {
  let component: BpLinesInputComponent;

  beforeEach(() => {
    component = new BpLinesInputComponent();
  });

  it("should parse valid manual lines with explicit and default directions", () => {
    component.rawInput = "PatMen HIGHER 31.5\nPrimmie, 38.5\ninvy lower 27.5";

    expect(component.parsedLines).toEqual([
      expect.objectContaining({ player: "PatMen", direction: "HIGHER", line: 31.5, valid: true }),
      expect.objectContaining({ player: "Primmie", direction: "HIGHER", line: 38.5, valid: true }),
      expect.objectContaining({ player: "invy", direction: "LOWER", line: 27.5, valid: true }),
    ]);
    expect(component.validCount).toBe(6);
    expect(component.errorCount).toBe(0);
  });

  it("should flag malformed manual lines without a name and line", () => {
    component.rawInput = "HIGHER\n31.5";

    expect(component.parsedLines.every(line => line.valid)).toBe(false);
    expect(component.validCount).toBe(3);
    expect(component.errorCount).toBe(2);
  });

  it("should reset manual and uploaded line state", () => {
    component.rawInput = "PatMen LOWER 31.5";
    component.uploaded = ["line-card.png"];
    component.status = "sent";

    component.reset();

    expect(component.rawInput).toBe("");
    expect(component.uploaded).toEqual([]);
    expect(component.combined).toEqual([]);
    expect(component.status).toBe("idle");
  });
});
