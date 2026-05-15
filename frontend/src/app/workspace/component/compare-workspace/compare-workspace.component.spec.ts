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
import { ActivatedRoute, convertToParamMap } from "@angular/router";
import { of } from "rxjs";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { CompareWorkspaceComponent } from "./compare-workspace.component";
import {
  OperatorPortCompareResult,
  WorkflowExecutionCompareSummary,
  WorkflowExecutionsService,
} from "../../../dashboard/service/user/workflow-executions/workflow-executions.service";

class ExecutionsServiceStub {
  compareTwoExecutions(): unknown {
    const summary: WorkflowExecutionCompareSummary = {
      wid: 1,
      eidA: 2,
      eidB: 3,
      operators: [],
    };
    return of(summary);
  }
  retrieveExecutionResultPage(): unknown {
    return of({ schema: [], rows: [], totalRowCount: 0, pageIndex: 0, pageSize: 25 });
  }
}

describe("CompareWorkspaceComponent", () => {
  let component: CompareWorkspaceComponent;
  let fixture: ComponentFixture<CompareWorkspaceComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CompareWorkspaceComponent, HttpClientTestingModule],
      providers: [
        { provide: WorkflowExecutionsService, useClass: ExecutionsServiceStub },
        {
          provide: ActivatedRoute,
          useValue: {
            snapshot: { paramMap: convertToParamMap({ wid: "1", eidA: "2", eidB: "3" }) },
          },
        },
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(CompareWorkspaceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("renders header with the workflow + execution ids from the route", () => {
    expect(component.wid).toBe(1);
    expect(component.eidA).toBe(2);
    expect(component.eidB).toBe(3);
  });

  it("classifies badges by status, schema match, and row count", () => {
    const shared: OperatorPortCompareResult = {
      operatorId: "op",
      portId: 0,
      status: "shared",
      rowCountA: 10,
      rowCountB: 10,
      schemaA: [],
      schemaB: [],
      schemaMatches: true,
    };
    expect(component.badgeClass(shared)).toBe("badge-green");

    expect(component.badgeClass({ ...shared, rowCountA: 9 })).toBe("badge-yellow");
    expect(component.badgeClass({ ...shared, schemaMatches: false })).toBe("badge-red");
    expect(component.badgeClass({ ...shared, status: "onlyInA" })).toBe("badge-red");
  });

  it("summarises schema diffs as adds / removes", () => {
    const entry: OperatorPortCompareResult = {
      operatorId: "op",
      portId: 0,
      status: "shared",
      rowCountA: 1,
      rowCountB: 1,
      schemaA: [
        { name: "id", typeName: "INTEGER" },
        { name: "old", typeName: "STRING" },
      ],
      schemaB: [
        { name: "id", typeName: "INTEGER" },
        { name: "new", typeName: "STRING" },
      ],
      schemaMatches: false,
    };
    const summary = component.schemaDiffSummary(entry);
    expect(summary).toContain("+ new");
    expect(summary).toContain("− old");
  });

  it("reports schemas match when both schemas are equal", () => {
    const entry: OperatorPortCompareResult = {
      operatorId: "op",
      portId: 0,
      status: "shared",
      rowCountA: 1,
      rowCountB: 1,
      schemaA: [{ name: "id", typeName: "INTEGER" }],
      schemaB: [{ name: "id", typeName: "INTEGER" }],
      schemaMatches: true,
    };
    expect(component.schemaDiffSummary(entry)).toBe("schemas match");
  });
});
