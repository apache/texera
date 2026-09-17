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
import { By } from "@angular/platform-browser";
import { BrowserAnimationsModule } from "@angular/platform-browser/animations";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NEVER, of, throwError } from "rxjs";
import { AgentRegistrationComponent } from "./agent-registration.component";
import { AgentService, ModelType } from "../../../../service/agent/agent.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { WorkflowActionService } from "../../../../service/workflow-graph/model/workflow-action.service";
import { ComputingUnitStatusService } from "../../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { ComputingUnitState } from "../../../../../common/type/computing-unit-connection.interface";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
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

    it("stringifies a rejection that is not an Error into the same message", () => {
      // The test above covers `error.message`; an rxjs source is free to reject
      // with anything, and a bare string has no `.message`, so the component
      // falls back to String(error) instead of interpolating `undefined`.
      fetchModelTypes.mockReturnValue(throwError(() => "backend unreachable"));
      fixture.detectChanges();

      expect(component.hasLoadingError).toBe(true);
      expect(notifyError).toHaveBeenCalledWith("Failed to fetch models: backend unreachable");
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

  describe("template rendering", () => {
    it("renders the spinner and its caption while the models are loading", () => {
      // never-emitting source: ngOnInit leaves isLoadingModels true
      fetchModelTypes.mockReturnValue(NEVER);
      fixture.detectChanges();

      expect(component.isLoadingModels).toBe(true);
      expect(fixture.debugElement.query(By.css("nz-spin"))).toBeTruthy();
      expect(fixture.nativeElement.textContent).toContain("Loading available models...");
      // the picker only appears once loading finishes
      expect(fixture.debugElement.query(By.css(".model-card"))).toBeNull();
    });

    it("renders a card per model type and selects the clicked one", () => {
      const second: ModelType = { ...MODEL, id: "other-model", name: "Other Model" };
      fetchModelTypes.mockReturnValue(of([MODEL, second]));
      fixture.detectChanges();

      const cards = fixture.debugElement.queryAll(By.css(".model-card"));
      expect(cards.length).toBe(2);
      expect(fixture.debugElement.query(By.css("nz-spin"))).toBeNull();

      cards[1].triggerEventHandler("click", new MouseEvent("click"));
      fixture.detectChanges();

      expect(component.selectedModelType).toBe("other-model");
      expect((cards[1].nativeElement as HTMLElement).classList).toContain("selected");
      expect((cards[0].nativeElement as HTMLElement).classList).not.toContain("selected");
    });

    it("writes what the user types in the name box back into customAgentName", async () => {
      // The name box is `[(ngModel)]`-bound. Every other test in this file only
      // assigns `customAgentName` on the instance, which is the direction the box
      // is never driven in; typing is what feeds the name createAgent() sends.
      fetchModelTypes.mockReturnValue(of([MODEL]));
      // The input is `[disabled]="!selectedModelType"`, so pick a model before the
      // first render; ngModel refuses to push a value into a disabled control.
      component.selectModelType(MODEL.id);
      fixture.detectChanges();
      // ngModel pushes the field into the DOM on a microtask rather than
      // synchronously inside detectChanges, so drain it before reading the box.
      await new Promise(resolve => setTimeout(resolve, 0));

      const input = fixture.debugElement.query(By.css("input[nz-input]")).nativeElement as HTMLInputElement;
      expect(input.disabled).toBe(false);
      expect(input.value).toBe("Texera Agent");

      // Typed rather than assigned -- the write-back is the untested direction.
      input.value = "My Analyst";
      input.dispatchEvent(new Event("input"));
      fixture.detectChanges();

      expect(component.customAgentName).toBe("My Analyst");
    });

    it("labels the submit button Creating... while a creation is in flight", () => {
      fetchModelTypes.mockReturnValue(of([MODEL]));
      fixture.detectChanges();

      const button = fixture.debugElement.query(By.css("button[nz-button]")).nativeElement as HTMLElement;
      expect(button.textContent).toContain("Create Agent");
      // The spinner is bound off the same flag as the label. Asserting only the
      // label leaves `[nzLoading]` free to be bound inverted, which would spin
      // the button whenever nothing is happening and stop spinning during the
      // one request it exists to cover.
      expect(button.classList).not.toContain("ant-btn-loading");

      component.isCreating = true;
      fixture.detectChanges();

      expect(button.textContent).toContain("Creating...");
      expect(button.textContent).not.toContain("Create Agent");
      expect(button.classList).toContain("ant-btn-loading");
    });

    it("keeps the submit button shut, and says why, until a model and a computing unit are both there", () => {
      // canCreate() is unit-tested above; what is pinned here is the template
      // actually honouring it. Nothing else in this file reads the button's
      // gate, so an inverted `[disabled]` binding -- Create enabled exactly
      // when creation is impossible -- passes every other test: createAgent()
      // re-checks selectedModelType and isCreating but never
      // computingUnitConnected, so the click would reach the backend.
      fetchModelTypes.mockReturnValue(of([MODEL]));
      fixture.detectChanges();

      const buttonEl = fixture.debugElement.query(By.css("button[nz-button]"));
      const button = buttonEl.nativeElement as HTMLButtonElement;
      const tooltip = buttonEl.injector.get(NzTooltipDirective);

      // getStatus defaults to Pending, so the gate legitimately starts shut.
      expect(component.canCreate()).toBe(false);
      expect(button.disabled).toBe(true);
      // Both arms of the tooltip ternary are already executed by this suite, so
      // only comparing the strings notices them being handed to the wrong side.
      expect(tooltip.title).toBe("Connect to a computing unit first");
      // The banner carries the same explanation. Its *ngIf arms are likewise
      // both executed by the suite, so only checking WHICH state renders it
      // notices the condition being inverted.
      expect(fixture.debugElement.query(By.css("nz-alert"))).toBeTruthy();

      component.computingUnitConnected = true;
      component.selectedModelType = MODEL.id;
      fixture.detectChanges();

      expect(component.canCreate()).toBe(true);
      expect(button.disabled).toBe(false);
      expect(tooltip.title).toBe("");
      expect(fixture.debugElement.query(By.css("nz-alert"))).toBeNull();
    });
  });
});
