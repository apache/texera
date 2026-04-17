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
import { UserProjectListItemComponent } from "./user-project-list-item.component";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { DashboardProject } from "../../../../type/dashboard-project.interface";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzModalService } from "ng-zorro-antd/modal";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { HighlightSearchTermsPipe } from "../../user-workflow/user-workflow-list-item/highlight-search-terms.pipe";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

describe("UserProjectListItemComponent", () => {
  let component: UserProjectListItemComponent;
  let fixture: ComponentFixture<UserProjectListItemComponent>;
  const januaryFirst1970 = 28800000; // 1970-01-01 in PST
  const testProject: DashboardProject = {
    color: null,
    creationTime: januaryFirst1970,
    description: "description",
    name: "project1",
    ownerId: 1,
    pid: 1,
    accessLevel: "WRITE",
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [UserProjectListItemComponent, HighlightSearchTermsPipe],
      providers: [
        NotificationService,
        UserProjectService,
        NzModalService,
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
      imports: [HttpClientTestingModule],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UserProjectListItemComponent);
    component = fixture.componentInstance;
    component.entry = testProject;
    component.editable = true;
    fixture.detectChanges();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
