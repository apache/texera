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

import { ComponentFixture, TestBed, fakeAsync, tick } from "@angular/core/testing";
import { Subject } from "rxjs";
import type { Mocked } from "vitest";

import { VisualizationFrameContentComponent } from "./visualization-frame-content.component";
import { WorkflowResultService } from "../../service/workflow-result/workflow-result.service";

describe("VisualizationFrameContentComponent", () => {
  const UPDATE_INTERVAL_MS = VisualizationFrameContentComponent.UPDATE_INTERVAL_MS;

  let fixture: ComponentFixture<VisualizationFrameContentComponent>;
  let component: VisualizationFrameContentComponent;
  let workflowResultService: Mocked<WorkflowResultService>;
  let resultUpdateStream: Subject<Record<string, unknown>>;

  // Each test fills in a snapshot via `setSnapshot`. The default returns [].
  let snapshotProvider: () => unknown[] | undefined;

  function makeResultSnapshot(html: string): unknown[] {
    return [{ "html-content": html }];
  }

  beforeEach(() => {
    resultUpdateStream = new Subject();
    snapshotProvider = () => undefined;

    const operatorResultServiceStub = {
      getCurrentResultSnapshot: () => snapshotProvider(),
    };

    const workflowResultServiceSpy = {
      getResultUpdateStream: vi.fn().mockReturnValue(resultUpdateStream.asObservable()),
      getResultService: vi.fn().mockReturnValue(operatorResultServiceStub),
    };

    TestBed.configureTestingModule({
      imports: [VisualizationFrameContentComponent],
      providers: [{ provide: WorkflowResultService, useValue: workflowResultServiceSpy }],
    });
    workflowResultService = TestBed.inject(WorkflowResultService) as unknown as Mocked<WorkflowResultService>;
  });

  // NB: do not use a default value for `operatorId` — `buildComponent(undefined)`
  // would then collapse to "op-1", masking the very behaviour we want to test.
  function buildComponent(operatorId: string | undefined): void {
    fixture = TestBed.createComponent(VisualizationFrameContentComponent);
    component = fixture.componentInstance;
    component.operatorId = operatorId;
    fixture.detectChanges(); // triggers ngAfterContentInit -> drawChart()
  }

  describe("drawChart() guard clauses", () => {
    it("is a no-op when operatorId is undefined", () => {
      buildComponent(undefined);

      expect(workflowResultService.getResultService).not.toHaveBeenCalled();
      expect(component.htmlData).toBe("");
    });

    it("is a no-op when getResultService returns undefined", () => {
      workflowResultService.getResultService.mockReturnValue(undefined);

      buildComponent("op-1");

      expect(component.htmlData).toBe("");
    });

    it("is a no-op when the snapshot is empty/undefined", () => {
      snapshotProvider = () => undefined;

      buildComponent("op-1");

      expect(component.htmlData).toBe("");
    });
  });

  describe("drawChart() rendering", () => {
    it("renders the last snapshot entry's html-content through the DomSanitizer", () => {
      snapshotProvider = () => [
        { "html-content": "<html><body><div>stale</div></body></html>" },
        { "html-content": "<html><body><div>fresh</div></body></html>" },
      ];

      buildComponent("op-1");

      // The sanitizer wraps the string in a SafeHtml; we don't peek inside the
      // wrapper, but the wrapped output must reflect the freshest entry.
      expect(component.htmlData).not.toBe("");
      // SafeHtml serialises back to the original markup via [srcdoc], so
      // assert on the iframe payload instead of the wrapper internals.
      const iframe = fixture.nativeElement.querySelector("iframe") as HTMLIFrameElement;
      expect(iframe.getAttribute("srcdoc")).toContain("fresh");
      expect(iframe.getAttribute("srcdoc")).not.toContain("stale");
    });
  });

  describe("result-update stream subscription", () => {
    it("redraws after UPDATE_INTERVAL_MS when a matching operatorId emits", fakeAsync(() => {
      snapshotProvider = () => makeResultSnapshot("<html><body><div>v1</div></body></html>");
      buildComponent("op-1");
      // Initial drawChart() from ngAfterContentInit ran with v1.
      const iframe = fixture.nativeElement.querySelector("iframe") as HTMLIFrameElement;
      expect(iframe.getAttribute("srcdoc")).toContain("v1");

      // Stream emits, but auditTime(UPDATE_INTERVAL_MS) holds the redraw.
      snapshotProvider = () => makeResultSnapshot("<html><body><div>v2</div></body></html>");
      resultUpdateStream.next({ "op-1": { fake: true } });

      // Before the audit window elapses, the iframe still shows v1.
      tick(UPDATE_INTERVAL_MS - 1);
      fixture.detectChanges();
      expect(iframe.getAttribute("srcdoc")).toContain("v1");

      // After the window, the redraw fires and the iframe flips to v2.
      tick(1);
      fixture.detectChanges();
      expect(iframe.getAttribute("srcdoc")).toContain("v2");
    }));

    it("ignores stream emissions that don't include the current operatorId", fakeAsync(() => {
      snapshotProvider = () => makeResultSnapshot("<html><body><div>only-v1</div></body></html>");
      buildComponent("op-1");
      const iframe = fixture.nativeElement.querySelector("iframe") as HTMLIFrameElement;
      const initialSrcdoc = iframe.getAttribute("srcdoc");

      snapshotProvider = () => makeResultSnapshot("<html><body><div>would-be-v2</div></body></html>");
      resultUpdateStream.next({ "some-other-op": { fake: true } });
      tick(UPDATE_INTERVAL_MS);
      fixture.detectChanges();

      // No redraw — the iframe srcdoc is unchanged.
      expect(iframe.getAttribute("srcdoc")).toBe(initialSrcdoc);
    }));

    it("ignores stream emissions when operatorId is undefined", fakeAsync(() => {
      buildComponent(undefined);
      const drawSpy = vi.spyOn(component, "drawChart");

      resultUpdateStream.next({ "op-1": { fake: true } });
      tick(UPDATE_INTERVAL_MS);

      expect(drawSpy).not.toHaveBeenCalled();
    }));
  });
});
