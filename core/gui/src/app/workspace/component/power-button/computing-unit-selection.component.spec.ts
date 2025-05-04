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
import { ComputingUnitSelectionComponent } from "./computing-unit-selection.component";
import { NzButtonModule } from "ng-zorro-antd/button";
import { CommonModule } from "@angular/common";
import { NzIconModule } from "ng-zorro-antd/icon";
import { WorkflowActionService } from "../../service/workflow-graph/model/workflow-action.service";
import { ExecuteWorkflowService } from "../../service/execute-workflow/execute-workflow.service";
import { OperatorMetadataService } from "../../service/operator-metadata/operator-metadata.service";
import { StubOperatorMetadataService } from "../../service/operator-metadata/stub-operator-metadata.service";
import { ActivatedRoute, ActivatedRouteSnapshot, convertToParamMap, Data, Params, UrlSegment } from "@angular/router";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule, NzModalService } from "ng-zorro-antd/modal";

describe("PowerButtonComponent", () => {
  let component: ComputingUnitSelectionComponent;
  let fixture: ComponentFixture<ComputingUnitSelectionComponent>;

  let activatedRouteMock: Partial<ActivatedRoute>;
  const activatedRouteSnapshotMock: Partial<ActivatedRouteSnapshot> = {
    queryParams: {},
    url: [] as UrlSegment[],
    params: {} as Params,
    fragment: null,
    data: {} as Data,
    paramMap: convertToParamMap({}),
    queryParamMap: convertToParamMap({}),
    outlet: "",
    routeConfig: null,
    root: null as any,
    parent: null as any,
    firstChild: null as any,
    children: [],
    pathFromRoot: [],
  };
  activatedRouteMock = {
    snapshot: activatedRouteSnapshotMock as ActivatedRouteSnapshot,
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ComputingUnitSelectionComponent], // Declare if not standalone
      imports: [
        HttpClientTestingModule, // Use TestingModule instead of HttpClientModule
        CommonModule,
        NzButtonModule,
        NzIconModule,
        NzDropDownModule,
        NzModalModule, // Add NzModalModule for the NzModalService
      ],
      providers: [
        { provide: ActivatedRoute, useValue: activatedRouteMock },
        NzModalService, // Add NzModalService provider
      ],
    }).compileComponents();

    fixture = TestBed.createComponent(ComputingUnitSelectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
