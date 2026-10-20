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
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { By } from "@angular/platform-browser";
import { NzModalModule } from "ng-zorro-antd/modal";
import { DEFAULT_HEIGHT, ResultPanelComponent } from "./result-panel.component";
import { ResultTableFrameComponent } from "./result-table-frame/result-table-frame.component";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";

describe("ResultPanelComponent docked placement", () => {
  let fixture: ComponentFixture<ResultPanelComponent>;
  let workspace: HTMLElement;

  beforeEach(async () => {
    for (const key of ["result-panel-width", "result-panel-height", "result-panel-style"]) localStorage.removeItem(key);
    await TestBed.configureTestingModule({
      imports: [ResultPanelComponent, HttpClientTestingModule, NoopAnimationsModule, NzModalModule],
      providers: [
        ...commonTestProviders,
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
      ],
    }).compileComponents();
    fixture = TestBed.createComponent(ResultPanelComponent);
    workspace = document.createElement("div");
    workspace.style.cssText = "position: fixed; inset: 0;";
    const anchor = document.createElement("div");
    // Match workspace.component.scss: the fixed panel's static position is the
    // bottom of the canvas, not the top of an arbitrary test host.
    anchor.style.cssText = "position: absolute; bottom: 0; left: 0;";
    workspace.appendChild(anchor);
    document.body.appendChild(workspace);
    anchor.appendChild(fixture.nativeElement);
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture?.destroy();
    workspace?.remove();
    for (const key of ["result-panel-width", "result-panel-height", "result-panel-style"]) localStorage.removeItem(key);
    TestBed.resetTestingModule();
  });

  const frame = () => new Promise<void>(resolve => requestAnimationFrame(() => resolve()));
  const panel = () => workspace.querySelector<HTMLElement>("#result-container")!;

  it("opens the entire default-height panel above its bottom canvas anchor", async () => {
    fixture.componentInstance.openPanel();
    fixture.detectChanges();
    await frame();
    expect(panel().getBoundingClientRect().height).toBe(DEFAULT_HEIGHT);
    expect(panel().getBoundingClientRect().top).toBeGreaterThanOrEqual(0);
    expect(panel().getBoundingClientRect().bottom).toBeLessThanOrEqual(workspace.getBoundingClientRect().bottom + 1);
  });

  it("keeps a real 50-row table's pagination visible when reset to its dock", async () => {
    const component = fixture.componentInstance;
    component.frameComponentConfigs.set("Result", {
      component: ResultTableFrameComponent,
      componentInputs: { operatorId: "placement-fixture" },
    });
    component.openPanel();
    component.resetPanelPosition();
    fixture.detectChanges();
    const table = fixture.debugElement.query(By.directive(ResultTableFrameComponent))
      .componentInstance as ResultTableFrameComponent;
    table.setupResultTable(
      Array.from({ length: 125 }, (_, i) => ({ id: i + 1, label: "東京" })),
      125
    );
    fixture.detectChanges();
    await frame();
    expect(panel().querySelectorAll("tr.table-row-hover").length).toBe(50);
    const pagination = panel().querySelector<HTMLElement>("nz-pagination")!;
    expect(pagination.getBoundingClientRect().bottom).toBeLessThanOrEqual(workspace.getBoundingClientRect().bottom + 1);
    const body = panel().querySelector<HTMLElement>(".ant-table-body")!;
    expect(body.clientHeight).toBeGreaterThan(0);
    expect(body.scrollHeight).toBeGreaterThan(body.clientHeight);
  });

  it("does not expose a full panel when closed, and reopens in the same valid dock", async () => {
    const component = fixture.componentInstance;
    component.closePanel();
    fixture.detectChanges();
    expect(panel().getBoundingClientRect().width).toBe(0);
    component.openPanel();
    fixture.detectChanges();
    await frame();
    expect(panel().getBoundingClientRect().bottom).toBeLessThanOrEqual(workspace.getBoundingClientRect().bottom + 1);
  });

  it.each([300, 700])("reopens a resized %ipx panel in its dock without accumulating offsets", async height => {
    const component = fixture.componentInstance;
    component.openPanel();
    component.onResize({ width: 800, height });
    await frame();
    component.resetPanelPosition();
    fixture.detectChanges();
    await frame();
    expect(panel().getBoundingClientRect().bottom).toBeCloseTo(workspace.getBoundingClientRect().bottom, 0);

    for (let attempt = 0; attempt < 2; attempt++) {
      component.closePanel();
      fixture.detectChanges();
      component.openPanel();
      fixture.detectChanges();
      await frame();
      expect(panel().getBoundingClientRect().height).toBe(DEFAULT_HEIGHT);
      expect(panel().getBoundingClientRect().bottom).toBeCloseTo(workspace.getBoundingClientRect().bottom, 0);
    }
  });

  it("resets an already-open resized panel to the full-height dock", async () => {
    const component = fixture.componentInstance;
    component.openPanel();
    component.onResize({ width: 600, height: 300 });
    await frame();
    component.resetPanelPosition();
    component.openPanel();
    fixture.detectChanges();
    await frame();
    expect(panel().getBoundingClientRect().height).toBe(DEFAULT_HEIGHT);
    expect(panel().getBoundingClientRect().bottom).toBeCloseTo(workspace.getBoundingClientRect().bottom, 0);
  });
});
