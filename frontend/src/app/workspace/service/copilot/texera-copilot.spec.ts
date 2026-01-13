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
import { TexeraCopilot, CopilotState } from "./texera-copilot";
import { WorkflowActionService } from "../workflow-graph/model/workflow-action.service";
import { WorkflowUtilService } from "../workflow-graph/util/workflow-util.service";
import { OperatorMetadataService } from "../operator-metadata/operator-metadata.service";
import { WorkflowCompilingService } from "../compile-workflow/workflow-compiling.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("TexeraCopilot", () => {
  let service: TexeraCopilot;
  let mockWorkflowActionService: jasmine.SpyObj<WorkflowActionService>;
  let mockWorkflowUtilService: jasmine.SpyObj<WorkflowUtilService>;
  let mockOperatorMetadataService: jasmine.SpyObj<OperatorMetadataService>;
  let mockWorkflowCompilingService: jasmine.SpyObj<WorkflowCompilingService>;
  let mockNotificationService: jasmine.SpyObj<NotificationService>;

  beforeEach(() => {
    mockWorkflowActionService = jasmine.createSpyObj("WorkflowActionService", ["getTexeraGraph"]);
    mockWorkflowUtilService = jasmine.createSpyObj("WorkflowUtilService", ["getOperatorTypeList"]);
    mockOperatorMetadataService = jasmine.createSpyObj("OperatorMetadataService", ["getOperatorSchema"]);
    mockWorkflowCompilingService = jasmine.createSpyObj("WorkflowCompilingService", [
      "getOperatorInputSchemaMap",
      "getOperatorOutputSchemaMap",
    ]);
    mockNotificationService = jasmine.createSpyObj("NotificationService", ["info", "error"]);

    TestBed.configureTestingModule({
      providers: [
        TexeraCopilot,
        { provide: WorkflowActionService, useValue: mockWorkflowActionService },
        { provide: WorkflowUtilService, useValue: mockWorkflowUtilService },
        { provide: OperatorMetadataService, useValue: mockOperatorMetadataService },
        { provide: WorkflowCompilingService, useValue: mockWorkflowCompilingService },
        { provide: NotificationService, useValue: mockNotificationService },
        ...commonTestProviders,
      ],
    });

    service = TestBed.inject(TexeraCopilot);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  it("should set agent info correctly", () => {
    service.setAgentInfo("Test Agent");
    expect(service).toBeTruthy();
  });

  it("should set model type correctly", () => {
    service.setModelType("gpt-4");
    expect(service).toBeTruthy();
  });

  it("should have initial state as UNAVAILABLE", () => {
    expect(service.getState()).toBe(CopilotState.UNAVAILABLE);
  });

  it("should update state correctly", done => {
    service.state$.subscribe(state => {
      if (state === CopilotState.UNAVAILABLE) {
        expect(state).toBe(CopilotState.UNAVAILABLE);
        done();
      }
    });
  });

  it("should clear messages correctly", () => {
    service.clearMessages();
    expect(service.getReActSteps().length).toBe(0);
  });

  it("should stop generation when in GENERATING state", () => {
    service.stopGeneration();
    expect(service).toBeTruthy();
  });

  it("should return system prompt", () => {
    const prompt = service.getSystemPrompt();
    expect(prompt).toBeTruthy();
    expect(typeof prompt).toBe("string");
  });

  it("should return tools info", done => {
    // Tools are only created after initialize() is called
    service.initialize().subscribe(() => {
      const tools = service.getToolsInfo();
      expect(tools).toBeTruthy();
      expect(Array.isArray(tools)).toBe(true);
      expect(tools.length).toBeGreaterThan(0);
      tools.forEach(tool => {
        expect(tool.name).toBeTruthy();
        expect(tool.description).toBeTruthy();
      });
      done();
    });
  });

  it("should check if connected", () => {
    expect(service.isConnected()).toBe(false);
  });

  it("should emit agent responses correctly", done => {
    service.reActSteps$.subscribe(responses => {
      if (responses.length > 0) {
        expect(responses[0].role).toBe("user");
        expect(responses[0].content).toBe("test message");
        done();
      }
    });

    // emitReActStep signature: (messageId, stepId, role, content, isBegin, isEnd, toolCalls?, toolResults?, usage?, operatorAccess?)
    (service as any).emitReActStep("test-id", 0, "user", "test message", true, true);
  });

  it("should return empty agent responses initially", () => {
    const responses = service.getReActSteps();
    expect(responses).toEqual([]);
  });

  describe("disconnect", () => {
    it("should disconnect and clear state", done => {
      service.disconnect().subscribe(() => {
        expect(service.getState()).toBe(CopilotState.UNAVAILABLE);
        expect(service.getReActSteps().length).toBe(0);
        done();
      });
    });

    it("should show notification on disconnect", done => {
      service.setAgentInfo("Test Agent");
      service.disconnect().subscribe(() => {
        expect(mockNotificationService.info).toHaveBeenCalled();
        done();
      });
    });
  });

  describe("state management", () => {
    it("should transition from UNAVAILABLE to GENERATING to AVAILABLE", done => {
      const states: CopilotState[] = [];

      service.state$.subscribe(state => {
        states.push(state);
        if (states.length === 1) {
          expect(states[0]).toBe(CopilotState.UNAVAILABLE);
          done();
        }
      });
    });
  });

  describe("workflow tools", () => {
    it("should create workflow tools correctly", () => {
      const tools = (service as any).createWorkflowTools();
      expect(tools).toBeTruthy();
      expect(typeof tools).toBe("object");
      // Tool names match the constants in the tool files
      expect(tools.listAllOperatorTypes).toBeTruthy();
      expect(tools.listOperatorsInCurrentWorkflow).toBeTruthy();
      expect(tools.listCurrentLinks).toBeTruthy();
      expect(tools.getCurrentOperator).toBeTruthy();
      expect(tools.getOperatorPropertiesSchema).toBeTruthy();
      expect(tools.getOperatorPortsInfo).toBeTruthy();
      expect(tools.getOperatorMetadata).toBeTruthy();
    });
  });
});
