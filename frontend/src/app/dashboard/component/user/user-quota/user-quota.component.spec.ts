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
import { UserQuotaComponent } from "./user-quota.component";
import { UserQuotaService } from "../../../service/user/quota/user-quota.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { of } from "rxjs";
import type { Mocked } from "vitest";
import { ExecutionQuota } from "../../../../common/type/user";

function execution(eid: number, result: number, runtime: number, log: number): ExecutionQuota {
  return {
    eid,
    workflowId: 1,
    workflowName: "wf-1",
    resultBytes: result,
    runTimeStatsBytes: runtime,
    logBytes: log,
  };
}

// Fixtures for the Cache Size comparator. The three byte triples are picked so that ordering by
// any single count — or by the sum with any one of the three terms dropped — yields a different
// sequence than the full sum does, and so that no two of the six orderings share a tie.
const SIZE_SORT_BIG = execution(30, 1, 450, 452); // 903
const SIZE_SORT_MIDDLE = execution(10, 800, 1, 1); // 802
const SIZE_SORT_SMALL = execution(20, 2, 700, 2); // 704

describe("UserQuotaComponent", () => {
  let component: UserQuotaComponent;
  let fixture: ComponentFixture<UserQuotaComponent>;
  let mockUserQuotaService: Mocked<UserQuotaService>;

  beforeEach(() => {
    mockUserQuotaService = {
      getCreatedDatasets: vi.fn(),
      getCreatedWorkflows: vi.fn(),
      getAccessWorkflows: vi.fn(),
      getExecutionQuota: vi.fn(),
      deleteExecutionCollection: vi.fn(),
    } as unknown as Mocked<UserQuotaService>;
    mockUserQuotaService.getCreatedDatasets.mockReturnValue(of([]));
    mockUserQuotaService.getCreatedWorkflows.mockReturnValue(of([]));
    mockUserQuotaService.getAccessWorkflows.mockReturnValue(of([]));
    mockUserQuotaService.getExecutionQuota.mockReturnValue(of([]));

    TestBed.configureTestingModule({
      providers: [{ provide: UserQuotaService, useValue: mockUserQuotaService }, ...commonTestProviders],
      imports: [UserQuotaComponent, HttpClientTestingModule],
    });

    fixture = TestBed.createComponent(UserQuotaComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
  });

  describe("sortBySize", () => {
    // Ascending, as NzTableSortFn requires (`a - b`): nz-table takes the comparator's result as-is
    // for 'ascend' and negates it for 'descend', so a `b - a` comparator lights the up caret while
    // rendering largest-first. nzSortDirections defaults to ['ascend', 'descend', null] and the
    // Cache Size header does not override it, so the first click is 'ascend'.
    it("orders executions by total cache size, smallest first", () => {
      const scrambled = [SIZE_SORT_MIDDLE, SIZE_SORT_SMALL, SIZE_SORT_BIG];

      expect([...scrambled].sort(component.sortBySize).map(e => e.eid)).toEqual([20, 10, 30]);
    });
  });
});
