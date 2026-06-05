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
import {
  FlameFrame,
  ProfilesQueryResponse,
} from "../../../../service/user/observability/observability.types";

describe("ProfilesPanelComponent", () => {
  let component: ProfilesPanelComponent;
  let fixture: ComponentFixture<ProfilesPanelComponent>;
  let mockService: { queryProfiles: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    mockService = { queryProfiles: vi.fn() };
    mockService.queryProfiles.mockReturnValue(of({ root: null, totalSamples: 0 } as ProfilesQueryResponse));
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

  it("populates root + totalSamples when the response has samples", () => {
    const root: FlameFrame = {
      name: "root",
      value: 100,
      children: [{ name: "f", value: 100, children: [] }],
    };
    mockService.queryProfiles.mockReturnValue(of({ root, totalSamples: 100 } as ProfilesQueryResponse));
    fixture.detectChanges();
    expect(component.root).toBe(root);
    expect(component.totalSamples).toBe(100);
  });

  it("leaves root null when the response has no samples (disabled-state branch)", () => {
    mockService.queryProfiles.mockReturnValue(of({ root: null, totalSamples: 0 } as ProfilesQueryResponse));
    fixture.detectChanges();
    expect(component.root).toBeNull();
    expect(component.totalSamples).toBe(0);
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
    component.root = { name: "stale", value: 1, children: [] };
    component.totalSamples = 99;
    component.refresh();
    expect(component.root).toBeNull();
    expect(component.totalSamples).toBe(0);
  });
});
