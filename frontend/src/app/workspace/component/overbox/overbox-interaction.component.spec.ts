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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import * as joint from "jointjs";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { OverboxService } from "../../service/overbox/overbox.service";
import { mockScanPredicate } from "../../service/workflow-graph/model/mock-workflow-data";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { OverboxInteractionComponent } from "./overbox-interaction.component";

describe("OverboxInteractionComponent", () => {
  let fixture: ComponentFixture<OverboxInteractionComponent>;
  let actions: WorkflowActionService;
  let overboxes: OverboxService;
  let paper: joint.dia.Paper;
  let surface: HTMLDivElement;
  let overboxID: string;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [OverboxInteractionComponent, HttpClientTestingModule],
      providers: [...commonTestProviders, { provide: OperatorMetadataService, useClass: StubOperatorMetadataService }],
    }).compileComponents();
    actions = TestBed.inject(WorkflowActionService);
    overboxes = TestBed.inject(OverboxService);
    surface = document.createElement("div");
    document.body.appendChild(surface);
    paper = actions.getJointGraphWrapper().attachMainJointPaper({ el: surface, width: 800, height: 600 });
    vi.spyOn(paper, "clientToLocalPoint").mockImplementation(point => point as joint.g.Point);
    actions.addOperator(mockScanPredicate, { x: 100, y: 100 });
    overboxID = overboxes.create("Input", "#6f8fb7", [mockScanPredicate.operatorID]);
    fixture = TestBed.createComponent(OverboxInteractionComponent);
    fixture.componentRef.setInput("paper", paper);
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture?.destroy();
    paper?.remove();
    surface?.remove();
  });

  it("shows only the resize tool when a section is clicked", () => {
    const sectionView = paper.findViewByModel(overboxID);
    paper.trigger("element:pointerdown", sectionView);
    expect(sectionView.hasTools("overbox-tools")).toBe(true);
    const tools = (sectionView as any)._toolsView.tools as joint.dia.ToolView[];
    expect(tools).toHaveLength(1);
    expect(tools[0]).toBeInstanceOf(joint.elementTools.Control);
  });

  it("keeps the title centered above the section and disables tools in read-only mode", () => {
    const section = actions.getJointGraph().getCell(overboxID) as joint.dia.Element;
    const sectionView = paper.findViewByModel(overboxID);
    expect(section.attr("label/refX")).toBeUndefined();
    expect(section.attr("label/refY")).toBeUndefined();
    expect(section.attr("label/x")).toBe(section.size().width / 2);
    expect(section.attr("label/y")).toBe(-10);
    expect(section.attr("label/textAnchor")).toBe("middle");

    fixture.componentRef.setInput("disabled", true);
    fixture.detectChanges();
    sectionView.removeTools();
    paper.trigger("element:pointerdown", sectionView);
    expect(sectionView.hasTools("overbox-tools")).toBe(false);
  });
});
