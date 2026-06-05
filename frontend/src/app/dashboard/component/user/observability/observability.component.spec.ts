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
import { of, throwError } from "rxjs";
import { ObservabilityComponent } from "./observability.component";
import { ObservabilityService } from "../../../service/user/observability/observability.service";
import { ObservabilityHealth } from "../../../service/user/observability/observability.types";

describe("ObservabilityComponent", () => {
  let component: ObservabilityComponent;
  let fixture: ComponentFixture<ObservabilityComponent>;
  let mockService: {
    health: ReturnType<typeof vi.fn>;
    logSources: ReturnType<typeof vi.fn>;
    searchLogs: ReturnType<typeof vi.fn>;
    queryMetrics: ReturnType<typeof vi.fn>;
    queryProfiles: ReturnType<typeof vi.fn>;
  };

  beforeEach(async () => {
    // The shell component constructs child panels (Logs, Metrics, etc.)
    // when /health reports a tab as reachable. Each child panel uses
    // ObservabilityService methods — stub the minimum surface so the
    // children mount without throwing during tab rendering.
    mockService = {
      health: vi.fn(),
      logSources: vi.fn().mockReturnValue(of({ services: [], workflowIds: [], computingUnitIds: [], userIds: [] })),
      searchLogs: vi.fn().mockReturnValue(of({ entries: [], total: 0 })),
      queryMetrics: vi.fn().mockReturnValue(of({ metric: "stub", points: [] })),
      queryProfiles: vi.fn().mockReturnValue(of({ root: null, totalSamples: 0 })),
    };
    await TestBed.configureTestingModule({
      imports: [ObservabilityComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [{ provide: ObservabilityService, useValue: mockService }],
    }).compileComponents();

    fixture = TestBed.createComponent(ObservabilityComponent);
    component = fixture.componentInstance;
  });

  it("renders all four tabs as reachable when /health says so", () => {
    const ok: ObservabilityHealth = {
      status: "ok",
      checks: { logs: true, metrics: true, traces: true, profiles: true },
    };
    mockService.health.mockReturnValue(of(ok));
    fixture.detectChanges();
    expect(component.isReachable("logs")).toBe(true);
    expect(component.isReachable("metrics")).toBe(true);
    expect(component.isReachable("traces")).toBe(true);
    expect(component.isReachable("profiles")).toBe(true);
  });

  it("marks individual tabs unreachable per the health response", () => {
    const partial: ObservabilityHealth = {
      status: "degraded",
      checks: { logs: true, metrics: false, traces: true, profiles: false },
    };
    mockService.health.mockReturnValue(of(partial));
    fixture.detectChanges();
    expect(component.isReachable("logs")).toBe(true);
    expect(component.isReachable("metrics")).toBe(false);
    expect(component.isReachable("profiles")).toBe(false);
  });

  it("falls back to 'all unreachable' + healthError flag when /health fails", () => {
    mockService.health.mockReturnValue(throwError(() => new Error("gateway down")));
    fixture.detectChanges();
    expect(component.healthError).toBe(true);
    expect(component.isReachable("logs")).toBe(false);
    expect(component.isReachable("metrics")).toBe(false);
    expect(component.isReachable("traces")).toBe(false);
    expect(component.isReachable("profiles")).toBe(false);
  });

  it("starts on the Logs tab (index 0)", () => {
    mockService.health.mockReturnValue(of({
      status: "ok",
      checks: { logs: true, metrics: true, traces: true, profiles: true },
    } as ObservabilityHealth));
    fixture.detectChanges();
    expect(component.activeTab).toBe(0);
  });
});
