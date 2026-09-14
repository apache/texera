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
import { ProfilesPanelComponent } from "./profiles-panel.component";
import { ObservabilityService } from "../../../../service/user/observability/observability.service";
import { ProfilesQueryResponse } from "../../../../service/user/observability/observability.types";

const EMPTY: ProfilesQueryResponse = { totalSamples: 0, top: [], timeline: [] };

describe("ProfilesPanelComponent", () => {
  let component: ProfilesPanelComponent;
  let fixture: ComponentFixture<ProfilesPanelComponent>;
  let mockService: { queryProfiles: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    mockService = { queryProfiles: vi.fn() };
    mockService.queryProfiles.mockReturnValue(of(EMPTY));
    await TestBed.configureTestingModule({
      imports: [ProfilesPanelComponent, HttpClientTestingModule, NoopAnimationsModule],
      providers: [{ provide: ObservabilityService, useValue: mockService }],
    }).compileComponents();
    fixture = TestBed.createComponent(ProfilesPanelComponent);
    component = fixture.componentInstance;
  });

  it("dispatches a queryProfiles call on init", () => {
    fixture.detectChanges();
    expect(mockService.queryProfiles).toHaveBeenCalledOnce();
  });

  it("populates top + timeline + totalSamples when the response has samples", () => {
    const resp: ProfilesQueryResponse = {
      totalSamples: 150,
      top: [
        { name: "(unsymbolized)", flat: 120 },
        { name: "foo", flat: 30 },
      ],
      timeline: [
        { timestampMs: 1000, value: 10 },
        { timestampMs: 2000, value: 20 },
      ],
    };
    mockService.queryProfiles.mockReturnValue(of(resp));
    fixture.detectChanges();
    expect(component.totalSamples).toBe(150);
    expect(component.top.length).toBe(2);
    expect(component.timeline.length).toBe(2);
    expect(component.topConsumer).toBe("(unsymbolized)");
    expect(component.hasData).toBe(true);
    expect(component.timelinePoints).not.toBe("");
  });

  it("has no data when the response is empty (disabled-state branch)", () => {
    mockService.queryProfiles.mockReturnValue(of(EMPTY));
    fixture.detectChanges();
    expect(component.totalSamples).toBe(0);
    expect(component.top.length).toBe(0);
    expect(component.hasData).toBe(false);
  });

  it("surfaces a human error message on HTTP failure", () => {
    mockService.queryProfiles.mockReturnValue(
      throwError(() => ({ error: { code: "backend_unreachable", message: "profiles backend down" } }))
    );
    fixture.detectChanges();
    expect(component.errorMessage).toBe("profiles backend down");
  });

  it("clears stale results before refreshing", () => {
    fixture.detectChanges();
    component.totalSamples = 99;
    component.top = [{ name: "stale", flat: 1 }];
    component.refresh();
    expect(component.totalSamples).toBe(0);
    expect(component.top.length).toBe(0);
  });
});
