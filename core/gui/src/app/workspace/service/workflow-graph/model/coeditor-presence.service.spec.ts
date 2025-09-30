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

import { CoeditorPresenceService } from "./coeditor-presence.service";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzDropdownMenuComponent, NzDropDownModule } from "ng-zorro-antd/dropdown";
import { CoeditorUserIconComponent } from "../../../component/menu/coeditor-user-icon/coeditor-user-icon.component";
import { WorkflowActionService } from "./workflow-action.service";
import { HttpClient } from "@angular/common/http";
import { commonTestProviders } from "../../../../common/testing/test-utils";

describe("CoeditorPresenceService", () => {
  let service: CoeditorPresenceService;
  let workflowActionService: WorkflowActionService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, NzDropDownModule],
      declarations: [CoeditorUserIconComponent],
      providers: [
        WorkflowActionService,
        CoeditorPresenceService,
        HttpClient,
        NzDropdownMenuComponent,
        ...commonTestProviders,
      ],
    });
    service = TestBed.inject(CoeditorPresenceService);
    workflowActionService = TestBed.inject(WorkflowActionService);
  });

  it("should be created", () => {
    expect(service).toBeTruthy();
  });
});
