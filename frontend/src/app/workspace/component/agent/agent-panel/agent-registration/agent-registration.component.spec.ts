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
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { of, throwError } from "rxjs";
import { AgentRegistrationComponent } from "./agent-registration.component";
import { AgentService, ModelType } from "../../../../service/agent/agent.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { ComputingUnitStatusService } from "../../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { ComputingUnitState } from "../../../../../common/type/computing-unit-connection.interface";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

const MODEL: ModelType = { id: "gpt", name: "GPT", description: "desc", icon: "robot" };

describe("AgentRegistrationComponent", () => {
  let fixture: ComponentFixture<AgentRegistrationComponent>;
  let component: AgentRegistrationComponent;

  let fetchModelTypes: ReturnType<typeof vi.fn>;
  let createAgent: ReturnType<typeof vi.fn>;
  let notifyError: ReturnType<typeof vi.fn>;
  let getWorkflowMetadata: ReturnType<typeof vi.fn>;
  let getStatus: ReturnType<typeof vi.fn>;

  beforeEach(async () => {
    // Safe defaults so `ngOnInit` (run on the first detectChanges) always has
    // observables to subscribe to; individual tests override before rendering.
    fetchModelTypes = vi.fn().mockReturnValue(of([MODEL]));
    createAgent = vi.fn();
    notifyError = vi.fn();
    getWorkflowMetadata = vi.fn().mockReturnValue({ wid: 123 });
    getStatus = vi.fn().mockReturnValue(of(ComputingUnitState.Pending));

    await TestBed.configureTestingModule({
      imports: [AgentRegistrationComponent, BrowserAnimationsModule, HttpClientTestingModule],
      providers: [
        { provide: AgentService, useValue: { fetchModelTypes, createAgent } },
        { provide: NotificationService, useValue: { error: notifyError, success: vi.fn() } },
        { provide: WorkflowActionService, useValue: { getWorkflowMetadata } },
        { provide: ComputingUnitStatusService, useValue: { getStatus } },
        ...commonTestProviders,
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(AgentRegistrationComponent);
    component = fixture.componentInstance;
  });

  it("should create and default customAgentName to 'Texera Agent'", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
    expect(component.customAgentName).toBe("Texera Agent");
  });

  describe("ngOnInit", () => {
    it("loads the fetched model types and clears the loading flag", () => {
      fetchModelTypes.mockReturnValue(of([MODEL]));
      fixture.detectChanges();

      expect(component.modelTypes).toEqual([MODEL]);
      expect(component.isLoadingModels).toBe(false);
      expect(component.hasLoadingError).toBe(false);
    });

    it("flags an error and notifies when no models are returned", () => {
      fetchModelTypes.mockReturnValue(of([]));
      fixture.detectChanges();

      expect(component.isLoadingModels).toBe(false);
      expect(component.hasLoadingError).toBe(true);
      expect(notifyError).toHaveBeenCalledWith(expect.stringContaining("No models available"));
    });

    it("flags an error and notifies when fetching models fails", () => {
      fetchModelTypes.mockReturnValue(throwError(() => new Error("boom")));
      fixture.detectChanges();

      expect(component.isLoadingModels).toBe(false);
      expect(component.hasLoadingError).toBe(true);
      expect(notifyError).toHaveBeenCalledWith("Failed to fetch models: boom");
    });

    it("marks the computing unit connected only when the status is Running", () => {
      getStatus.mockReturnValue(of(ComputingUnitState.Running));
      fixture.detectChanges();
      expect(component.computingUnitConnected).toBe(true);
    });

    it("marks the computing unit disconnected for a non-Running status", () => {
      getStatus.mockReturnValue(of(ComputingUnitState.NoComputingUnit));
      fixture.detectChanges();
      expect(component.computingUnitConnected).toBe(false);
    });
  });

  describe("selectModelType", () => {
    it("updates the selected model type", () => {
      component.selectModelType("claude");
      expect(component.selectedModelType).toBe("claude");
    });
  });

  describe("createAgent", () => {
    it("emits agentCreated with the new id and resets the form on success", () => {
      component.selectedModelType = "gpt";
      createAgent.mockReturnValue(of({ id: "agent-1" }));
      const emitted: string[] = [];
      component.agentCreated.subscribe(id => emitted.push(id));

      component.createAgent();

      expect(createAgent).toHaveBeenCalledWith("gpt", "Texera Agent", 123);
      expect(emitted).toEqual(["agent-1"]);
      expect(component.selectedModelType).toBeNull();
      expect(component.isCreating).toBe(false);
    });

    it("passes undefined as the name when customAgentName is blank", () => {
      component.selectedModelType = "gpt";
      component.customAgentName = "";
      createAgent.mockReturnValue(of({ id: "agent-2" }));

      component.createAgent();

      expect(createAgent).toHaveBeenCalledWith("gpt", undefined, 123);
    });

    it("does nothing when no model type is selected", () => {
      component.selectedModelType = null;
      component.createAgent();
      expect(createAgent).not.toHaveBeenCalled();
    });

    it("does nothing when a creation is already in progress", () => {
      component.selectedModelType = "gpt";
      component.isCreating = true;
      component.createAgent();
      expect(createAgent).not.toHaveBeenCalled();
    });

    it("notifies and clears isCreating when creation fails", () => {
      component.selectedModelType = "gpt";
      createAgent.mockReturnValue(throwError(() => "network down"));

      component.createAgent();

      expect(notifyError).toHaveBeenCalledWith(expect.stringContaining("Failed to create agent"));
      expect(component.isCreating).toBe(false);
    });
  });

  describe("canCreate", () => {
    it("is true only with a selected model, an idle form, and a connected computing unit", () => {
      component.selectedModelType = "gpt";
      component.isCreating = false;
      component.computingUnitConnected = true;
      expect(component.canCreate()).toBe(true);

      component.computingUnitConnected = false;
      expect(component.canCreate()).toBe(false);

      component.computingUnitConnected = true;
      component.selectedModelType = null;
      expect(component.canCreate()).toBe(false);
    });
  });
});
