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

import { ChangeDetectorRef, SimpleChange, SimpleChanges } from "@angular/core";
import { DomSanitizer, SafeHtml } from "@angular/platform-browser";
import { of } from "rxjs";
import { AgentInteractionComponent } from "./agent-interaction.component";
import { AgentService, OperatorResultMode, SampleRow } from "../../../service/agent/agent.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";

/**
 * These tests exercise the component's pure presentation logic (visualization
 * caching, column/row derivation) by constructing it directly with lightweight
 * stubbed dependencies, so no Angular template/DI bootstrapping is required.
 */
describe("AgentInteractionComponent", () => {
  let component: AgentInteractionComponent;

  const agentService = {
    getAllAgents: () => of([]),
    agentChange$: of(null),
    getActivelyConnectedAgentIds: () => [],
    isAgentActivelyConnected: () => false,
    sendMessage: () => {},
  } as unknown as AgentService;

  const workflowActionService = {} as unknown as WorkflowActionService;
  const notificationService = {} as unknown as NotificationService;
  const changeDetectorRef = { detectChanges: () => {} } as unknown as ChangeDetectorRef;
  // Echo the html back so we can assert the cached value flows through.
  const sanitizer = {
    bypassSecurityTrustHtml: (html: string) => html as unknown as SafeHtml,
  } as unknown as DomSanitizer;

  function row(rowIndex: number, tuple: Record<string, any>): SampleRow {
    return { rowIndex, tuple };
  }

  beforeEach(() => {
    component = new AgentInteractionComponent(
      agentService,
      workflowActionService,
      notificationService,
      changeDetectorRef,
      sanitizer
    );
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("ngOnChanges - visualization html caching", () => {
    it("caches sanitized html when a visualization tuple carries html-content", () => {
      component.sampleTuples = [row(0, { "html-content": "<h1>chart</h1>" })];
      component.ngOnChanges({ sampleTuples: new SimpleChange(undefined, component.sampleTuples, true) });

      expect(component.getVisualizationHtml()).toEqual("<h1>chart</h1>" as unknown as SafeHtml);
    });

    it("keeps the cached html when the content is unchanged across calls", () => {
      component.sampleTuples = [row(0, { "html-content": "<p>same</p>" })];
      const changes: SimpleChanges = { sampleTuples: new SimpleChange(undefined, component.sampleTuples, true) };

      component.ngOnChanges(changes);
      component.ngOnChanges(changes); // identical html -> unchanged branch, cache reused

      expect(component.getVisualizationHtml()).toEqual("<p>same</p>" as unknown as SafeHtml);
    });

    it("clears the cached html when no html-content is present", () => {
      component.sampleTuples = [row(0, { "html-content": "<p>x</p>" })];
      component.ngOnChanges({ resultMode: new SimpleChange(undefined, OperatorResultMode.VISUALIZATION, true) });
      // Now switch to a tuple with no html-content.
      component.sampleTuples = [row(0, { value: 1 })];
      component.ngOnChanges({ sampleTuples: new SimpleChange(undefined, component.sampleTuples, false) });

      expect(component.getVisualizationHtml()).toEqual("" as unknown as SafeHtml);
    });

    it("ignores changes unrelated to sampleTuples/resultMode", () => {
      component.ngOnChanges({ operatorId: new SimpleChange(undefined, "op-1", true) });
      expect(component.getVisualizationHtml()).toEqual("" as unknown as SafeHtml);
    });
  });

  describe("isVisualization", () => {
    it("is true only in visualization mode", () => {
      component.resultMode = OperatorResultMode.VISUALIZATION;
      expect(component.isVisualization()).toBe(true);

      component.resultMode = OperatorResultMode.TABLE;
      expect(component.isVisualization()).toBe(false);
    });
  });

  describe("getSampleColumns", () => {
    it("returns the keys of the first tuple", () => {
      component.sampleTuples = [row(0, { a: 1, b: 2 })];
      expect(component.getSampleColumns()).toEqual(["a", "b"]);
    });

    it("returns an empty array when there are no sample tuples", () => {
      component.sampleTuples = [];
      expect(component.getSampleColumns()).toEqual([]);

      component.sampleTuples = undefined;
      expect(component.getSampleColumns()).toEqual([]);
    });

    it("maps a column header to its own name", () => {
      expect(component.getColumnDisplayName("colX")).toBe("colX");
    });
  });

  describe("getDisplayRows", () => {
    it("returns an empty array when there are no sample tuples", () => {
      component.sampleTuples = [];
      expect(component.getDisplayRows()).toEqual([]);
    });

    it("returns rows without ellipsis when indices are contiguous", () => {
      component.sampleTuples = [row(0, { a: 1 }), row(1, { a: 2 })];
      const rows = component.getDisplayRows();
      expect(rows.length).toBe(2);
      expect(rows.every(r => !r.isEllipsis)).toBe(true);
    });

    it("inserts an ellipsis marker when there is a gap between row indices", () => {
      component.sampleTuples = [row(0, { a: 1 }), row(5, { a: 2 })];
      const rows = component.getDisplayRows();

      expect(rows.length).toBe(3);
      expect(rows[1].isEllipsis).toBe(true);
      expect(rows[1].row).toBeUndefined();
      expect(rows[2].row?.rowIndex).toBe(5);
    });
  });
});
