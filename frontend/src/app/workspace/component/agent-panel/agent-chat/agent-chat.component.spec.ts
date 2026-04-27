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
import { AgentChatComponent } from "./agent-chat.component";
import { AgentInfo, TexeraCopilotManagerService } from "../../../service/copilot/texera-copilot-manager.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { NO_ERRORS_SCHEMA } from "@angular/core";
import { CopilotState, TexeraCopilot } from "../../../service/copilot/texera-copilot";
import { of } from "rxjs";

describe("AgentChatComponent", () => {
  let component: AgentChatComponent;
  let fixture: ComponentFixture<AgentChatComponent>;
  let mockCopilotManagerService: jasmine.SpyObj<TexeraCopilotManagerService>;
  let mockNotificationService: jasmine.SpyObj<NotificationService>;

  beforeEach(async () => {
    mockCopilotManagerService = jasmine.createSpyObj("TexeraCopilotManagerService", [
      "getReActStepsObservable",
      "getAgentStateObservable",
      "sendMessage",
      "stopGeneration",
      "clearMessages",
      "getSystemInfo",
    ]);
    mockCopilotManagerService.getReActStepsObservable.and.returnValue(of([]));
    mockCopilotManagerService.getAgentStateObservable.and.returnValue(of(CopilotState.AVAILABLE));
    mockCopilotManagerService.getSystemInfo.and.returnValue(of({ systemPrompt: "", tools: [] }));
    mockNotificationService = jasmine.createSpyObj("NotificationService", ["info", "error", "success"]);

    await TestBed.configureTestingModule({
      declarations: [AgentChatComponent],
      imports: [HttpClientTestingModule],
      providers: [
        { provide: TexeraCopilotManagerService, useValue: mockCopilotManagerService },
        { provide: NotificationService, useValue: mockNotificationService },
        ...commonTestProviders,
      ],
      schemas: [NO_ERRORS_SCHEMA],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AgentChatComponent);
    component = fixture.componentInstance;
    component.agentInfo = {
      id: "agent-1",
      name: "Test Agent",
      modelType: "gpt-4o-mini",
      instance: {} as TexeraCopilot,
      createdAt: new Date(0),
    } satisfies AgentInfo;
  });

  it("should create", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });
});
