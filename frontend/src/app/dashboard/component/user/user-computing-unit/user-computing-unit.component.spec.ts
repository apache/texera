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
import { UserComputingUnitComponent } from "./user-computing-unit.component";
import { NzCardModule } from "ng-zorro-antd/card";
import { NzModalService } from "ng-zorro-antd/modal";
import { HttpClient } from "@angular/common/http";
import { UserService } from "../../../../common/service/user/user.service";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";
import { WorkflowComputingUnitManagingService } from "../../../../common/service/computing-unit/workflow-computing-unit/workflow-computing-unit-managing.service";
import { ComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/computing-unit-status.service";
import { MockComputingUnitStatusService } from "../../../../common/service/computing-unit/computing-unit-status/mock-computing-unit-status.service";

describe("UserComputingUnitComponent", () => {
  let component: UserComputingUnitComponent;
  let fixture: ComponentFixture<UserComputingUnitComponent>;
  let mockComputingUnitService: jasmine.SpyObj<WorkflowComputingUnitManagingService>;

  beforeEach(async () => {
    mockComputingUnitService = jasmine.createSpyObj<WorkflowComputingUnitManagingService>([
      "getComputingUnitTypes",
      "getComputingUnitLimitOptions",
      "createKubernetesBasedComputingUnit",
      "createLocalComputingUnit",
    ]);

    await TestBed.configureTestingModule({
      declarations: [UserComputingUnitComponent],
      providers: [
        NzModalService,
        HttpClient,
        { provide: UserService, useClass: StubUserService },
        { provide: WorkflowComputingUnitManagingService, useValue: mockComputingUnitService },
        { provide: ComputingUnitStatusService, useClass: MockComputingUnitStatusService },
        ...commonTestProviders,
      ],
      imports: [NzCardModule],
    }).compileComponents();

    fixture = TestBed.createComponent(UserComputingUnitComponent);
    component = fixture.componentInstance;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
