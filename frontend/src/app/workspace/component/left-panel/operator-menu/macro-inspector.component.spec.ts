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
import { MacroDefinition, expandMacros, macroInstance } from "../../../../common/util/workflow-macro";
import { OperatorPredicate } from "../../../types/workflow-common.interface";
import { OperatorState } from "../../../types/execute-workflow.interface";
import { mockSentimentPredicate } from "../../../service/workflow-graph/model/mock-workflow-data";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { WorkflowStatusService } from "../../../service/workflow-status/workflow-status.service";
import { WorkflowWebsocketService } from "../../../service/workflow-websocket/workflow-websocket.service";
import { EMPTY } from "rxjs";

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
        JointUIService,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        WorkflowStatusService,
        { provide: WorkflowWebsocketService, useValue: { websocketEvent: () => EMPTY } },
        { provide: NotificationService, useValue: notification },
      ],
    });
  });

  const runtimeFixture = () => {
    const wrap = (id: string, operators: OperatorPredicate[]): MacroDefinition => ({
      ...definition,
      id,
      name: id,
      content: {
        ...definition.content,
        operators,
        macroInterface: {
          id,
          inputs: operators.map((op, index) => ({
            portID: `input-${index}`,
            targets: [{ operatorID: op.operatorID, portID: op.inputPorts[0].portID }],
          })),
          outputs: operators.map((op, index) => ({
            portID: `output-${index}`,
            targets: [{ operatorID: op.operatorID, portID: op.outputPorts[0].portID }],
          })),
        },
      },
    });
    const leaf = wrap("leaf", [mockSentimentPredicate]);
    const middle = wrap("middle", [macroInstance(leaf, "same")]);
    const outer = wrap("outer", [macroInstance(middle, "left"), macroInstance(middle, "right")]);
    data.definition = outer;
    data.dependencies = [leaf, middle, outer];
    data.library = data.dependencies;
    TestBed.inject(OperatorMetadataService).registerMacros(data.dependencies);
    const expanded = expandMacros({
      ...outer.content,
      macroInterface: undefined,
      operators: [macroInstance(outer, "one"), macroInstance(outer, "two")],
      macroDependencies: data.dependencies,
    });
    data.runtime = { operatorID: "one", getExpansion: () => expanded };
    const status = TestBed.inject(WorkflowStatusService);
    const updates = Object.fromEntries(
      Object.entries(expanded.paths).map(([id, path]) => {
        const rows = path[0] === "two" ? 100 : path[1] === "left" ? 4 : 7;
        return [
          id,
          {
            operatorState: OperatorState.Running,
            aggregatedInputRowCount: rows,
            aggregatedOutputRowCount: rows,
            inputPortMetrics: { "0": rows },
            outputPortMetrics: { "0": rows },
            numWorkers: 1,
            aggregatedDataProcessingTime: 2_000_000,
          },
        ];
      })
    );
    status.setExternalStatus(updates);
    const fixture = TestBed.createComponent(MacroInspectorComponent);
    fixture.detectChanges();
    const paper = () => (fixture.componentInstance as any).paper;
    const count = (id: string) => paper().getModelById(id).portProp("input-0", "attrs/.port-label/text");
    const drill = (id: string) => {
      paper().trigger("element:pointerdblclick", { model: paper().getModelById(id) });
      fixture.detectChanges();
    };
    return { fixture, status, updates, count, drill, paper };
  };

  it("shows the selected instance's existing and live statistics through three levels and back", () => {
    const { fixture, status, updates, count, drill } = runtimeFixture();
    expect(count("left")).toBe("4");
    expect(count("right")).toBe("7");
    drill("left");
    expect(count("same")).toBe("4");
    drill("same");
    expect(count(mockSentimentPredicate.operatorID)).toBe("4");
    expect(fixture.nativeElement.textContent).toContain("Runtime · 2 ms");
    const next = Object.fromEntries(
      Object.entries(updates).map(([id, s]) => [
        id,
        {
          ...s,
          operatorState: OperatorState.Completed,
          aggregatedInputRowCount: 0,
          aggregatedOutputRowCount: 0,
          inputPortMetrics: {},
          outputPortMetrics: {},
        },
      ])
    );
    status.setExternalStatus(next);
    fixture.detectChanges();
    expect(count(mockSentimentPredicate.operatorID)).toBe("0");
    expect(fixture.nativeElement.textContent).toContain("Completed");
    fixture.componentInstance.back();
    fixture.componentInstance.back();
    fixture.detectChanges();
    expect(count("left")).toBe("0");
    expect(count("right")).toBe("0");
    status.clearStatus();
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).not.toContain("#workers: 1");
    expect(fixture.nativeElement.textContent).not.toContain("Completed");
    fixture.destroy();
    const markup = fixture.nativeElement.innerHTML;
    status.setExternalStatus(updates);
    expect(fixture.nativeElement.innerHTML).toBe(markup);
  });

  it("clears stale statistics when runtime context disappears and detaches an editable copy", async () => {
    const { fixture, status, updates, count, drill } = runtimeFixture();
    expect(count("left")).toBe("4");
    const getExpansion = data.runtime.getExpansion;
    data.runtime.getExpansion = () => undefined;
    status.setExternalStatus(updates);
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).not.toContain("#workers: 1");
    data.runtime.getExpansion = getExpansion;
    status.setExternalStatus(updates);
    fixture.detectChanges();
    expect(count("left")).toBe("4");
    macros.copyDefinition.mockRejectedValue(new Error("save failed"));
    await fixture.componentInstance.copy();
    expect(count("left")).toBe("4");
    expect(notification.error).toHaveBeenCalledWith("save failed");
    const copy = {
      ...data.definition,
      id: "copy",
      content: {
        ...data.definition.content,
        macroInterface: { ...data.definition.content.macroInterface, id: "copy" },
      },
    };
    macros.copyDefinition.mockResolvedValue(copy);
    await fixture.componentInstance.copy();
    fixture.detectChanges();
    status.setExternalStatus(updates);
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).not.toContain("Running");
    expect(fixture.componentInstance.definition).toBe(copy);
    drill("left");
    status.setExternalStatus(updates);
    fixture.detectChanges();
    expect(fixture.nativeElement.textContent).not.toContain("Running");
    expect(fixture.nativeElement.textContent).toContain("No execution statistics for this view");
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
