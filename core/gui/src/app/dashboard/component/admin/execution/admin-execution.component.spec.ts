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

import { ComponentFixture, inject, TestBed, waitForAsync } from "@angular/core/testing";
import { AdminExecutionComponent } from "./admin-execution.component";
import { AdminExecutionService } from "../../../service/admin/execution/admin-execution.service";
import { HttpClientTestingModule, HttpTestingController } from "@angular/common/http/testing";
import { NzDropDownModule } from "ng-zorro-antd/dropdown";
import { NzModalModule } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("AdminDashboardComponent", () => {
  let component: AdminExecutionComponent;
  let fixture: ComponentFixture<AdminExecutionComponent>;

  beforeEach(waitForAsync(() => {
    TestBed.configureTestingModule({
      declarations: [AdminExecutionComponent],
      providers: [AdminExecutionService, ...commonTestProviders],
      imports: [HttpClientTestingModule, NzDropDownModule, NzModalModule],
    }).compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(AdminExecutionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it("should create", inject([HttpTestingController], () => {
    expect(component).toBeTruthy();
  }));
});
