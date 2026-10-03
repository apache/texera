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

import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { WorkflowGraph } from "../../../service/workflow-graph/model/workflow-graph";
import {
  mockPoint,
  mockScanPredicate,
  mockSentimentPredicate,
} from "../../../service/workflow-graph/model/mock-workflow-data";
import { OperatorColorPickerComponent } from "./operator-color-picker.component";

describe("OperatorColorPickerComponent", () => {
  let fixture: ComponentFixture<OperatorColorPickerComponent>;
  let action: WorkflowActionService;
  let graph: WorkflowGraph;
  const ids = [mockScanPredicate.operatorID, mockSentimentPredicate.operatorID];
  let data: { operatorIDs: string[] };
  const buttons = () => [...fixture.nativeElement.querySelectorAll("button")] as HTMLButtonElement[];
  const button = (name: string) => buttons().find(el => el.textContent?.trim() === name)!;

  beforeEach(async () => {
    data = { operatorIDs: [...ids] };
    await TestBed.configureTestingModule({
      imports: [OperatorColorPickerComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: data },
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        ...commonTestProviders,
      ],
    }).compileComponents();
    action = TestBed.inject(WorkflowActionService);
    graph = action.getTexeraGraph() as WorkflowGraph;
    action.addOperator(structuredClone(mockScanPredicate), mockPoint);
    action.addOperator(structuredClone(mockSentimentPredicate), { x: 400, y: 100 });
    fixture = TestBed.createComponent(OperatorColorPickerComponent);
    fixture.detectChanges();
  });

  afterEach(() => graph.destroyYModel());

  it("renders seven named accessible choices, with Default initially pressed", () => {
    expect(buttons().map(el => el.textContent?.trim())).toEqual([
      "Default",
      "Yellow",
      "Blue",
      "Green",
      "Pink",
      "Purple",
      "Orange",
    ]);
    expect(fixture.nativeElement.querySelector('[role="group"]')?.getAttribute("aria-label")).toBe("Operator color");
    expect(button("Default").getAttribute("aria-pressed")).toBe("true");
    expect(button("Blue").getAttribute("aria-pressed")).toBe("false");
    expect(fixture.nativeElement.textContent).toContain("2 selected operators");
    expect(fixture.nativeElement.textContent).toContain("Heat maps");
  });

  it("applies a choice to all dialog targets and resets it", () => {
    button("Blue").click();
    fixture.detectChanges();
    expect(ids.map(id => graph.getOperator(id).color)).toEqual(["blue", "blue"]);
    expect(button("Blue").getAttribute("aria-pressed")).toBe("true");
    button("Default").click();
    fixture.detectChanges();
    expect(ids.every(id => !graph.getSharedOperatorType(id).has("color"))).toBe(true);
    expect(button("Default").getAttribute("aria-pressed")).toBe("true");
  });

  it("shows no pressed color when the selection contains mixed values", () => {
    action.setOperatorsColor([ids[0]], "blue");
    fixture.detectChanges();
    expect(buttons().every(el => el.getAttribute("aria-pressed") === "false")).toBe(true);
    button("Green").click();
    fixture.detectChanges();
    expect(button("Green").getAttribute("aria-pressed")).toBe("true");
  });

  it("edits the original dialog targets even if canvas selection changes", () => {
    action.getJointGraphWrapper().unhighlightOperators(...ids);
    button("Pink").click();
    expect(ids.map(id => graph.getOperator(id).color)).toEqual(["pink", "pink"]);
  });

  it("disables choices if execution locks the workflow after opening", () => {
    action.disableWorkflowModification();
    fixture.detectChanges();
    expect(buttons().every(el => el.disabled)).toBe(true);
    fixture.componentInstance.setColor("blue");
    expect(graph.getOperator(ids[0])).not.toHaveProperty("color");
  });

  it("disables choices for readonly workflow metadata", () => {
    action.setWorkflowMetadata({ ...action.getWorkflowMetadata(), readonly: true });
    fixture.detectChanges();
    expect(buttons().every(el => el.disabled)).toBe(true);
    fixture.componentInstance.setColor("blue");
    expect(graph.getOperator(ids[0])).not.toHaveProperty("color");
  });

  it("does not partially edit if a coeditor removes a target while the dialog is open", () => {
    action.deleteOperatorsAndLinks([ids[1]]);
    fixture.detectChanges();
    expect(buttons().every(el => el.disabled)).toBe(true);
    fixture.componentInstance.setColor("blue");
    expect(graph.getOperator(ids[0])).not.toHaveProperty("color");
  });

  it("disables an empty selection", () => {
    data.operatorIDs = [];
    fixture.detectChanges();
    expect(buttons().every(el => el.disabled)).toBe(true);
    expect(buttons().every(el => el.getAttribute("aria-pressed") === "false")).toBe(true);
  });
});
