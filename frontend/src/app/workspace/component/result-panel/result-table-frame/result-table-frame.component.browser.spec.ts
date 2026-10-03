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
import { provideZoneChangeDetection } from "@angular/core";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { NzModalModule } from "ng-zorro-antd/modal";
import { ResultTableFrameComponent } from "./result-table-frame.component";
import { OperatorMetadataService } from "../../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../../service/operator-metadata/stub-operator-metadata.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { MockGuiConfigService } from "../../../../common/service/gui-config.service.mock";
import { NzIconService } from "ng-zorro-antd/icon";
import { CloudDownloadOutline, EyeOutline, PlayCircleOutline, SoundOutline } from "@ant-design/icons-angular/icons";
import { ResultCellIconComponent } from "./result-cell-icon.component";

// Layout assertions require real Chromium: jsdom has no scrolling or element geometry.
describe("ResultTableFrameComponent scrolling layout", () => {
  let fixture: ComponentFixture<ResultTableFrameComponent>;
  let container: HTMLDivElement;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ResultTableFrameComponent, HttpClientTestingModule, NoopAnimationsModule, NzModalModule],
      providers: [
        ...commonTestProviders,
        provideZoneChangeDetection(),
        { provide: OperatorMetadataService, useClass: StubOperatorMetadataService },
      ],
    }).compileComponents();
    fixture = TestBed.createComponent(ResultTableFrameComponent);
    container = document.createElement("div");
    container.style.cssText = "width: 700px; height: 450px;";
    document.body.appendChild(container);
    container.appendChild(fixture.nativeElement);
    fixture.componentInstance.operatorId = "scroll-test";
    fixture.detectChanges();
    fixture.componentInstance.setupResultTable(
      Array.from({ length: 60 }, (_, index) => ({ name: `row ${index + 1}`, count: index })),
      60
    );
    fixture.detectChanges();
    await new Promise<void>(resolve => requestAnimationFrame(() => resolve()));
  });

  afterEach(() => {
    fixture?.destroy();
    container?.remove();
    TestBed.resetTestingModule();
  });

  it("scrolls a 50-row page while keeping the header and pagination within the panel", async () => {
    const body = container.querySelector<HTMLElement>(".ant-table-body")!;
    expect(body).not.toBeNull();
    expect(container.querySelectorAll("tbody tr.table-row-hover").length).toBe(50);
    expect(body.clientHeight).toBeGreaterThan(0);
    expect(body.scrollHeight).toBeGreaterThan(body.clientHeight);
    const header = container.querySelector<HTMLElement>(".ant-table-header")!;
    const headerTop = header.getBoundingClientRect().top;
    body.scrollTop = body.scrollHeight;
    await new Promise<void>(resolve => requestAnimationFrame(() => resolve()));
    expect(body.scrollTop).toBeGreaterThan(0);
    expect(header.getBoundingClientRect().top).toBe(headerTop);
    const lastRow = body.querySelector("tr.table-row-hover:last-child")!;
    expect(lastRow.textContent).toContain("row 50");
    expect(lastRow.getBoundingClientRect().bottom).toBeLessThanOrEqual(body.getBoundingClientRect().bottom + 1);
    const pagination = container.querySelector("nz-pagination")!;
    expect(pagination.getBoundingClientRect().bottom).toBeLessThanOrEqual(container.getBoundingClientRect().bottom + 1);
  });

  it("shrinks the scroll viewport, not the page size, when the panel becomes shorter", async () => {
    const body = container.querySelector<HTMLElement>(".ant-table-body")!;
    const oldHeight = body.clientHeight;
    container.style.height = "300px";
    await new Promise<void>(resolve => requestAnimationFrame(() => resolve()));
    expect(body.clientHeight).toBeGreaterThan(0);
    expect(body.clientHeight).toBeLessThan(oldHeight);
    expect(fixture.componentInstance.pageSize).toBe(50);
    expect(container.querySelectorAll("tbody tr.table-row-hover").length).toBe(50);
  });

  it.each([50, 100])("paginates a wide %i-row page with per-cell exports enabled", async pageSize => {
    (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
      exportExecutionResultEnabled: true,
    });
    const component = fixture.componentInstance;
    component.pageSize = pageSize;
    component.setupResultTable(
      Array.from({ length: 125 }, (_, index) =>
        Object.fromEntries(Array.from({ length: 25 }, (_, column) => [`col_${column}`, index + 1]))
      ),
      125
    );
    fixture.detectChanges();
    await new Promise<void>(resolve => requestAnimationFrame(() => resolve()));
    expect(container.querySelectorAll(".download-button").length).toBe(pageSize * 25);
    container.querySelector<HTMLElement>(".ant-pagination-next")!.click();
    fixture.detectChanges();
    await new Promise<void>(resolve => requestAnimationFrame(() => resolve()));
    expect(component.currentPageIndex).toBe(2);
    expect(container.querySelector("tr.table-row-hover td .cell-content")!.textContent!.trim()).toBe(
      String(pageSize + 1)
    );
    expect(container.querySelectorAll(".download-button").length).toBe(Math.min(pageSize, 125 - pageSize) * 25);
  });

  it.each([true, false])("bounds loaded cell-icon work with exports %s", async exportsEnabled => {
    // Unresolved HTTP icon requests do not execute NzIcon's post-load change detection.
    // Register the real icon so this exercises the same path as the application.
    TestBed.inject(NzIconService).addIcon(CloudDownloadOutline, EyeOutline, PlayCircleOutline, SoundOutline);
    (TestBed.inject(GuiConfigService) as unknown as MockGuiConfigService).setConfig({
      exportExecutionResultEnabled: exportsEnabled,
    });
    const component = fixture.componentInstance;
    component.pageSize = 100;
    fixture.autoDetectChanges();
    let cellChecks = 0;
    const classify = component.getCellMediaType.bind(component);
    component.getCellMediaType = (...args) => {
      cellChecks++;
      return classify(...args);
    };
    component.setupResultTable(
      Array.from({ length: 125 }, (_, index) =>
        Object.fromEntries(
          Array.from({ length: 25 }, (_, column) => [
            `col_${column}`,
            ["https://example.test/image.png", "https://example.test/audio.wav", "https://example.test/video.mp4"][
              column
            ] ?? index + 1,
          ])
        )
      ),
      125
    );
    component.tableStats = { col_3: { min: 1, max: 125, not_null_count: 125 } };
    component.prevTableStats = component.tableStats;
    fixture.detectChanges();
    await new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
    expect(container.querySelectorAll(".download-button texera-result-cell-icon").length).toBe(
      exportsEnabled ? 2500 : 0
    );
    expect(container.querySelectorAll(".cell-content texera-result-cell-icon").length).toBe(300);
    expect(container.querySelectorAll("tbody tr.table-row-hover").length).toBe(100);
    // Allow generous fixed change-detection overhead, but reject quadratic work.
    expect(cellChecks).toBeLessThan(100 * 25 * 20);
  });

  it("decodes every cell icon and clears missing or malformed icon names", async () => {
    const icon = TestBed.createComponent(ResultCellIconComponent);
    try {
      for (const type of ["cloud-download", "eye", "sound", "play-circle"]) {
        icon.componentRef.setInput("type", type);
        icon.detectChanges();
        const mask = (icon.nativeElement.querySelector("span") as HTMLElement).style.maskImage;
        const image = new Image();
        image.src = JSON.parse(mask.slice(4, -1));
        await image.decode();
        expect(image.naturalWidth).toBeGreaterThan(0);
        expect(icon.nativeElement.getAttribute("aria-label")).toBe(type);
      }
      for (const type of [undefined, null, "", "../../invalid", "__proto__", 'eye");url(https://invalid.test)']) {
        icon.componentRef.setInput("type", type);
        icon.detectChanges();
        expect((icon.nativeElement.querySelector("span") as HTMLElement).style.maskImage).toBe("");
      }
    } finally {
      icon.destroy();
    }
  });

  it("starts the next page at the top after scrolling to the end of a page", async () => {
    const body = container.querySelector<HTMLElement>(".ant-table-body")!;
    body.scrollTop = body.scrollHeight;
    expect(body.scrollTop).toBeGreaterThan(0);
    container.querySelector<HTMLElement>(".ant-pagination-next")!.click();
    fixture.detectChanges();
    await new Promise<void>(resolve => requestAnimationFrame(() => resolve()));
    expect(fixture.componentInstance.currentPageIndex).toBe(2);
    expect(body.scrollTop).toBe(0);
    expect(body.querySelector("tr.table-row-hover")!.textContent).toContain("row 51");
  });

  it("keeps statistics, column navigation and a wide table inside a short panel", async () => {
    const component = fixture.componentInstance;
    component.columnLimit = 2;
    component.currentColumnOffset = 2;
    component.tableStats = { count: { min: 0, max: 59, not_null_count: 60 } };
    component.prevTableStats = component.tableStats;
    container.style.height = "300px";
    container.style.width = "400px";
    fixture.detectChanges();
    await new Promise<void>(resolve => requestAnimationFrame(() => resolve()));
    const body = container.querySelector<HTMLElement>(".ant-table-body")!;
    expect(body.clientHeight).toBeGreaterThan(0);
    expect(body.scrollHeight).toBeGreaterThan(body.clientHeight);
    expect(container.querySelector(".statsLine")!.getBoundingClientRect().height).toBeGreaterThan(0);
    expect(container.querySelector("nz-pagination")!.getBoundingClientRect().bottom).toBeLessThanOrEqual(
      container.getBoundingClientRect().bottom + 1
    );
  });

  it.each([400, 800])("aligns data with all six statistics headers in a %ipx panel", async width => {
    container.style.width = `${width}px`;
    const component = fixture.componentInstance;
    component.setupResultTable(
      Array.from({ length: 125 }, (_, i) => ({
        id: i + 1,
        category: i % 2 ? "東京" : "A",
        x: i + 1,
        z: i % 7,
        y: 1.7 * i,
        group: i % 2 ? "日本" : "Alpha",
      })),
      125
    );
    component.tableStats = Object.fromEntries(
      ["id", "category", "x", "z", "y", "group"].map(name => [name, { min: 1, max: 125, not_null_count: 125 }])
    );
    component.prevTableStats = component.tableStats;
    fixture.detectChanges();
    await new Promise<void>(resolve => requestAnimationFrame(() => resolve()));
    const headers = container.querySelectorAll<HTMLElement>(".ant-table-header thead tr:first-child th");
    const cells = container.querySelector(".ant-table-body tr.table-row-hover")!.querySelectorAll<HTMLElement>("td");
    expect(cells.length).toBe(6);
    for (let i = 0; i < cells.length; i++) {
      expect(Math.abs(headers[i].getBoundingClientRect().left - cells[i].getBoundingClientRect().left)).toBeLessThan(2);
      expect(Math.abs(headers[i].getBoundingClientRect().width - cells[i].getBoundingClientRect().width)).toBeLessThan(
        2
      );
    }
    const body = container.querySelector<HTMLElement>(".ant-table-body")!;
    const header = container.querySelector<HTMLElement>(".ant-table-header")!;
    expect(body.scrollWidth).toBeGreaterThan(body.clientWidth);
    body.scrollLeft = body.scrollWidth;
    body.dispatchEvent(new Event("scroll"));
    await new Promise<void>(resolve => requestAnimationFrame(() => requestAnimationFrame(() => resolve())));
    expect(body.scrollLeft).toBeGreaterThan(0);
    expect(Math.abs(header.scrollLeft - body.scrollLeft)).toBeLessThan(2);
    expect(Math.abs(headers[5].getBoundingClientRect().left - cells[5].getBoundingClientRect().left)).toBeLessThan(2);
  });
});
