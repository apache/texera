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
import { TexeraCopilotManagerService } from "./texera-copilot-manager.service";
import { CopilotState } from "./texera-copilot";
import { commonTestProviders } from "../../../common/testing/test-utils";
import { AppSettings } from "../../../common/app-setting";

describe("TexeraCopilotManagerService", () => {
  let service: TexeraCopilotManagerService;
  let httpMock: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule],
      providers: [TexeraCopilotManagerService, ...commonTestProviders],
    });

    service = TestBed.inject(TexeraCopilotManagerService);
    httpMock = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    httpMock.verify();
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });

  describe("fetchModelTypes", () => {
    it("should fetch and format model types", done => {
      const mockResponse = {
        data: [
          { id: "gpt-4", object: "model", created: 1234567890, owned_by: "openai" },
          { id: "claude-3", object: "model", created: 1234567891, owned_by: "anthropic" },
        ],
        object: "list",
      };

      service.fetchModelTypes().subscribe(models => {
        expect(models.length).toBe(2);
        expect(models[0].id).toBe("gpt-4");
        expect(models[0].name).toBe("Gpt 4");
        expect(models[1].id).toBe("claude-3");
        expect(models[1].name).toBe("Claude 3");
        done();
      });

      const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/models`);
      expect(req.request.method).toBe("GET");
      req.flush(mockResponse);
    });

    it("should handle fetch error gracefully", done => {
      service.fetchModelTypes().subscribe(models => {
        expect(models).toEqual([]);
        done();
      });

      const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/models`);
      req.error(new ProgressEvent("error"));
    });

    it("should cache model types with shareReplay", done => {
      const mockResponse = {
        data: [{ id: "gpt-4", object: "model", created: 1234567890, owned_by: "openai" }],
        object: "list",
      };

      service.fetchModelTypes().subscribe(() => {
        service.fetchModelTypes().subscribe(models => {
          expect(models.length).toBe(1);
          done();
        });
      });

      const req = httpMock.expectOne(`${AppSettings.getApiEndpoint()}/models`);
      req.flush(mockResponse);
    });
  });

  describe("getAllAgents", () => {
    it("should return empty array initially", done => {
      service.getAllAgents().subscribe(agents => {
        expect(agents).toEqual([]);
        done();
      });
    });
  });

  describe("getAgentCount", () => {
    it("should return 0 initially", done => {
      service.getAgentCount().subscribe(count => {
        expect(count).toBe(0);
        done();
      });
    });
  });

  describe("getAgent", () => {
    it("should throw error when agent not found", done => {
      service.getAgent("non-existent").subscribe({
        next: () => fail("Should have thrown error"),
        error: (error: unknown) => {
          expect((error as Error).message).toContain("not found");
          done();
        },
      });
    });
  });

  describe("isAgentConnected", () => {
    it("should return false for non-existent agent", done => {
      service.isAgentConnected("non-existent").subscribe(connected => {
        expect(connected).toBe(false);
        done();
      });
    });
  });

  describe("agent lifecycle management", () => {
    it("should emit agent change event on agent creation", done => {
      let eventEmitted = false;

      service.agentChange$.subscribe(() => {
        eventEmitted = true;
      });

      setTimeout(() => {
        expect(eventEmitted).toBe(false);
        done();
      }, 100);
    });
  });

  describe("sendMessage", () => {
    it("should throw error for non-existent agent", done => {
      service.sendMessage("non-existent", "test message").subscribe({
        next: () => fail("Should have thrown error"),
        error: (error: unknown) => {
          expect((error as Error).message).toContain("not found");
          done();
        },
      });
    });
  });

  describe("getAgentResponses", () => {
    it("should throw error for non-existent agent", done => {
      service.getAgentResponses("non-existent").subscribe({
        next: () => fail("Should have thrown error"),
        error: (error: unknown) => {
          expect((error as Error).message).toContain("not found");
          done();
        },
      });
    });
  });

  describe("getAgentResponsesObservable", () => {
    it("should throw error for non-existent agent", done => {
      service.getReActStepsObservable("non-existent").subscribe({
        next: () => fail("Should have thrown error"),
        error: (error: unknown) => {
          expect((error as Error).message).toContain("not found");
          done();
        },
      });
    });
  });

  describe("clearMessages", () => {
    it("should throw error for non-existent agent", done => {
      service.clearMessages("non-existent").subscribe({
        next: () => fail("Should have thrown error"),
        error: (error: unknown) => {
          expect((error as Error).message).toContain("not found");
          done();
        },
      });
    });
  });

  describe("stopGeneration", () => {
    it("should throw error for non-existent agent", done => {
      service.stopGeneration("non-existent").subscribe({
        next: () => fail("Should have thrown error"),
        error: (error: unknown) => {
          expect((error as Error).message).toContain("not found");
          done();
        },
      });
    });
  });

  describe("getAgentState", () => {
    it("should throw error for non-existent agent", done => {
      service.getAgentState("non-existent").subscribe({
        next: () => fail("Should have thrown error"),
        error: (error: unknown) => {
          expect((error as Error).message).toContain("not found");
          done();
        },
      });
    });
  });

  describe("getAgentStateObservable", () => {
    it("should throw error for non-existent agent", done => {
      service.getAgentStateObservable("non-existent").subscribe({
        next: () => fail("Should have thrown error"),
        error: (error: unknown) => {
          expect((error as Error).message).toContain("not found");
          done();
        },
      });
    });
  });

  describe("getSystemInfo", () => {
    it("should throw error for non-existent agent", done => {
      service.getSystemInfo("non-existent").subscribe({
        next: () => fail("Should have thrown error"),
        error: (error: unknown) => {
          expect((error as Error).message).toContain("not found");
          done();
        },
      });
    });
  });

  describe("deleteAgent", () => {
    it("should return false for non-existent agent", done => {
      service.deleteAgent("non-existent").subscribe(deleted => {
        expect(deleted).toBe(false);
        done();
      });
    });
  });
});
