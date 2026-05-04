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

import { CoeditorUserIconComponent } from "./coeditor-user-icon.component";
import { CoeditorPresenceService } from "../../../service/workflow-graph/model/coeditor-presence.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import { HttpClient } from "@angular/common/http";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropdownMenuComponent, NzDropDownModule } from "ng-zorro-antd/dropdown";
import { StubUserService } from "../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("CoeditorUserIconComponent", () => {
  let component: CoeditorUserIconComponent;
  let fixture: ComponentFixture<CoeditorUserIconComponent>;
  let coeditorPresenceService: CoeditorPresenceService;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [CoeditorUserIconComponent, HttpClientTestingModule, NzDropDownModule],
      providers: [
        WorkflowActionService,
        CoeditorPresenceService,
        HttpClient,
        NzDropdownMenuComponent,
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(CoeditorUserIconComponent);
    component = fixture.componentInstance;
    coeditorPresenceService = TestBed.inject(CoeditorPresenceService);
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
