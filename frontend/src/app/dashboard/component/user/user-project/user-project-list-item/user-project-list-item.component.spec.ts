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

import { Component, ViewChild } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { UserProjectListItemComponent } from "./user-project-list-item.component";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { DashboardProject } from "../../../../type/dashboard-project.interface";
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NzListComponent } from "ng-zorro-antd/list";
import { NzModalService } from "ng-zorro-antd/modal";
import { provideRouter } from "@angular/router";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../../common/testing/test-utils";

// UserProjectListItemComponent is rooted at <nz-list-item>; instantiating it
// outside an <nz-list> host throws "No provider found for NzListComponent".
@Component({
  standalone: true,
  imports: [NzListComponent, UserProjectListItemComponent],
  template: `
    <nz-list>
      <texera-user-project-list-item
        [entry]="entry"
        [editable]="editable"></texera-user-project-list-item>
    </nz-list>
  `,
})
class TestHostComponent {
  entry!: DashboardProject;
  editable = true;
  @ViewChild(UserProjectListItemComponent, { static: true }) inner!: UserProjectListItemComponent;
}

describe("UserProjectListItemComponent", () => {
  let component: UserProjectListItemComponent;
  let hostFixture: ComponentFixture<TestHostComponent>;
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
      imports: [TestHostComponent, HttpClientTestingModule],
      providers: [
        NotificationService,
        UserProjectService,
        NzModalService,
        { provide: UserService, useClass: StubUserService },
        provideRouter([]),
        ...commonTestProviders,
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    hostFixture = TestBed.createComponent(TestHostComponent);
    hostFixture.componentInstance.entry = testProject;
    hostFixture.componentInstance.editable = true;
    hostFixture.detectChanges();
    component = hostFixture.componentInstance.inner;
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });
});
