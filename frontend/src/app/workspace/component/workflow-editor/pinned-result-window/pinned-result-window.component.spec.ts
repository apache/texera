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

import { Component, Input } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { By } from "@angular/platform-browser";
import { PinnedResultWindowComponent } from "./pinned-result-window.component";
import { ResultTableFrameComponent } from "../../result-panel/result-table-frame/result-table-frame.component";
import { VisualizationFrameContentComponent } from "../../visualization-panel-content/visualization-frame-content.component";
import { OperatorResultPreviewComponent } from "../operator-result-preview/operator-result-preview.component";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import {
  OperatorPaginationResultService,
  WorkflowResultService,
} from "../../../service/workflow-result/workflow-result.service";
import { OperatorPredicate, PortDescription } from "../../../types/workflow-common.interface";

// Stand-ins for the frames the window holds, so the test needs none of their services.
@Component({ selector: "texera-result-table-frame", template: "", standalone: true })
class StubTableFrame {
  @Input() operatorId?: string;
  @Input() portIndex = 0;
  @Input() containerHeight?: number;
}

@Component({ selector: "texera-visualization-panel-content", template: "", standalone: true })
class StubChartFrame {
  @Input() operatorId?: string;
}

@Component({ selector: "texera-operator-result-preview", template: "", standalone: true })
class StubPreview {
  @Input() operatorId?: string;
}

describe("PinnedResultWindowComponent", () => {
  let fixture: ComponentFixture<PinnedResultWindowComponent>;
  let operator: OperatorPredicate;
  let hasTable: boolean;

  const ports = (...names: (string | undefined)[]): PortDescription[] =>
    names.map((displayName, i) => ({ portID: `output-${i}`, displayName }));

  beforeEach(async () => {
    operator = { operatorID: "op1", operatorType: "Split", outputPorts: ports(undefined) } as OperatorPredicate;
    hasTable = true;
    const graph = { hasOperator: () => true, getOperator: () => operator };

    await TestBed.configureTestingModule({
      imports: [PinnedResultWindowComponent],
      providers: [
        { provide: WorkflowActionService, useValue: { getTexeraGraph: () => graph } },
        {
          provide: WorkflowResultService,
          useValue: {
            getPaginatedResultService: () => (hasTable ? ({} as OperatorPaginationResultService) : undefined),
            getResultService: () => undefined,
          },
        },
      ],
    })
      .overrideComponent(PinnedResultWindowComponent, {
        remove: {
          imports: [ResultTableFrameComponent, VisualizationFrameContentComponent, OperatorResultPreviewComponent],
        },
        add: { imports: [StubTableFrame, StubChartFrame, StubPreview] },
      })
      .compileComponents();

    fixture = TestBed.createComponent(PinnedResultWindowComponent);
    fixture.componentRef.setInput("operatorId", "op1");
  });

  const tabs = () => fixture.debugElement.queryAll(By.css(".pinned-result__port"));
  const table = () => fixture.debugElement.query(By.directive(StubTableFrame)).componentInstance as StubTableFrame;

  it("shows no port tabs for an operator with one output", () => {
    fixture.detectChanges();

    expect(tabs().length).toBe(0);
    expect(table().portIndex).toBe(0);
  });

  it("shows a tab for each output, named as on the canvas or by its place, the first one shown", () => {
    operator = { ...operator, outputPorts: ports("False", "True") };
    fixture.detectChanges();
    expect(tabs().map(tab => (tab.nativeElement as HTMLElement).textContent?.trim())).toEqual(["False", "True"]);

    operator = { ...operator, outputPorts: ports(undefined, "") };
    fixture.detectChanges();
    expect(tabs().map(tab => (tab.nativeElement as HTMLElement).textContent?.trim())).toEqual(["Output 1", "Output 2"]);
    expect(tabs()[0].classes["pinned-result__port--active"]).toBe(true);
    expect(table().portIndex).toBe(0);
  });

  it("shows the output whose tab is clicked", () => {
    operator = { ...operator, outputPorts: ports("False", "True") };
    fixture.detectChanges();

    (tabs()[1].nativeElement as HTMLElement).click();
    fixture.detectChanges();

    expect(table().portIndex).toBe(1);
    expect(table().operatorId).toBe("op1");
    expect(tabs()[1].attributes["aria-selected"]).toBe("true");
    expect(tabs()[0].attributes["aria-selected"]).toBe("false");
  });

  it("hands the table the height its body leaves, not the result panel's", () => {
    fixture.detectChanges();
    fixture.componentInstance.tableHeight = 400;
    fixture.detectChanges();

    expect(table().containerHeight).toBe(400);
  });

  it("shows no port tabs when the result is not a table", () => {
    operator = { ...operator, outputPorts: ports("False", "True") };
    hasTable = false;
    fixture.detectChanges();

    expect(tabs().length).toBe(0);
  });
});
