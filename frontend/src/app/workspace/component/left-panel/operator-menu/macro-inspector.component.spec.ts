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

import { TestBed } from "@angular/core/testing";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { MacroInspectorComponent } from "./macro-inspector.component";
import { MacroService } from "../../../service/macro/macro.service";
import { JointUIService } from "../../../service/joint-ui/joint-ui.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { ExecutionMode } from "../../../../common/type/workflow";

describe("MacroInspectorComponent", () => {
  const definition = {
    id: "macro",
    version: 8,
    wid: 12,
    name: "Clean",
    content: {
      operators: [],
      links: [],
      operatorPositions: {},
      commentBoxes: [],
      settings: { executionMode: ExecutionMode.PIPELINED, dataTransferBatchSize: 100 },
      macroInterface: { id: "macro", inputs: [], outputs: [] },
    },
  };
  let data: any, macros: any, notification: any;
  beforeEach(() => {
    data = { definition, dependencies: [], library: [definition] };
    macros = { copyDefinition: vi.fn(), library$: { value: [] } };
    notification = { error: vi.fn(), success: vi.fn() };
    TestBed.configureTestingModule({
      imports: [MacroInspectorComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: data },
        { provide: MacroService, useValue: macros },
        { provide: JointUIService, useValue: {} },
        { provide: NotificationService, useValue: notification },
      ],
    });
  });

  it("links only to an unambiguous accessible definition and preserves its canvas on navigation", () => {
    const fixture = TestBed.createComponent(MacroInspectorComponent);
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector("a").getAttribute("href")).toBe("/user/workflow/12");
    fixture.componentInstance.stack.push(definition);
    fixture.componentInstance.back();
    expect(fixture.nativeElement.querySelector("svg")).toBeTruthy();
    data.library = [];
    fixture.detectChanges();
    expect(fixture.nativeElement.querySelector("a")).toBeNull();
  });

  it("reports failed copies and resets the busy state without changing the pinned definition", async () => {
    macros.copyDefinition.mockRejectedValue(new Error("save failed"));
    const fixture = TestBed.createComponent(MacroInspectorComponent);
    fixture.detectChanges();
    await fixture.componentInstance.copy();
    expect(notification.error).toHaveBeenCalledWith("save failed");
    expect(fixture.componentInstance.copying).toBe(false);
    expect(fixture.componentInstance.definition).toBe(definition);
  });

  it("identifies the pinned snapshot and labels its graph and empty properties", () => {
    const fixture = TestBed.createComponent(MacroInspectorComponent);
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).toContain("Pinned version");
    expect(fixture.nativeElement.textContent).toContain("No operators in this definition");
    expect(fixture.nativeElement.querySelector('[aria-label="Macro graph"]')).toBeTruthy();
    expect(fixture.nativeElement.querySelector("a").textContent).toContain("Edit latest definition");
  });
});
