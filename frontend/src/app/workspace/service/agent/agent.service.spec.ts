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
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { AgentService, AgentInfo } from "./agent.service";
import { NotificationService } from "../../../common/service/notification/notification.service";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { ComputingUnitStatusService } from "../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { DashboardWorkflowComputingUnit } from "../../../common/type/workflow-computing-unit";
import { commonTestProviders } from "../../../common/testing/test-utils";

describe("AgentService", () => {
  let service: AgentService;
  let httpMock: HttpTestingController;
  let selectedUnit: DashboardWorkflowComputingUnit | null;

  const apiAgent = {
    id: "agent-1",
    name: "Bob",
    modelType: "gpt-5-mini",
    state: "AVAILABLE",
    createdAt: "2026-06-11T00:00:00.000Z",
  };

  beforeEach(() => {
    selectedUnit = null;
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [
        AgentService,
        { provide: NotificationService, useValue: { error: () => {}, success: () => {}, info: () => {} } },
        { provide: WorkflowPersistService, useValue: {} },
        {
          provide: ComputingUnitStatusService,
          useValue: { getSelectedComputingUnitValue: () => selectedUnit },
        },
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(AgentService);
    httpMock = TestBed.inject(HttpTestingController);
    // The constructor syncs the local agent cache with the backend.
    httpMock.expectOne(req => req.method === "GET" && req.url === "/api/agents").flush({ agents: [] });
  });

  afterEach(() => {
    httpMock.verify();
  });

  describe("createAgent", () => {
    it("creates an agent without putting the user token in the payload", () => {
      let created: AgentInfo | undefined;
      service.createAgent("gpt-5-mini", "Bob").subscribe(agent => (created = agent));

      const req = httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents");
      expect(req.request.body.userToken).toBeUndefined();
      expect(req.request.body.modelType).toEqual("gpt-5-mini");
      expect(req.request.body.name).toEqual("Bob");
      expect(req.request.body.workflowId).toBeUndefined();
      expect(req.request.body.computingUnitId).toBeUndefined();
      req.flush(apiAgent);

      expect(created?.id).toEqual("agent-1");
      expect(created?.modelType).toEqual("gpt-5-mini");
    });

    it("includes workflowId and the selected computing unit id in the payload", () => {
      selectedUnit = { computingUnit: { cuid: 7 } } as unknown as DashboardWorkflowComputingUnit;
      service.createAgent("gpt-5-mini", "Bob", 42).subscribe();

      const req = httpMock.expectOne(r => r.method === "POST" && r.url === "/api/agents");
      expect(req.request.body.workflowId).toEqual(42);
      expect(req.request.body.computingUnitId).toEqual(7);
      expect(req.request.body.userToken).toBeUndefined();
      req.flush(apiAgent);
    });
  });
});
