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

import { Component, EventEmitter, Input, Output } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { of, Subject } from "rxjs";
import { UserProjectComponent } from "./user-project.component";
import { UserProjectListItemComponent } from "./user-project-list-item/user-project-list-item.component";
import { PublicProjectComponent } from "./public-project/public-project.component";
import { UserProjectService } from "../../../service/user/project/user-project.service";
import { DashboardProject } from "../../../type/dashboard-project.interface";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UserService } from "../../../../common/service/user/user.service";
import { MOCK_USER, MOCK_USER_ID, StubUserService } from "../../../../common/service/user/stub-user.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { commonTestProviders } from "../../../../common/testing/test-utils";

// Minimal stand-in for the heavyweight list-item child (pulls in ngx-markdown,
// color-picker, share-access, etc.). It keeps the same selector and declares the
// inputs/outputs the parent template binds so change-detection compiles.
@Component({
  selector: "texera-user-project-list-item",
  standalone: true,
  template: "",
})
class StubUserProjectListItemComponent {
  @Input() entry?: DashboardProject;
  @Input() editable = false;
  @Input() uid?: number;
  @Output() deleted = new EventEmitter<void>();
  @Output() refresh = new EventEmitter<void>();
}

describe("UserProjectComponent", () => {
  let fixture: ComponentFixture<UserProjectComponent>;
  let component: UserProjectComponent;

  let projectServiceMock: {
    getProjectList: ReturnType<typeof vi.fn>;
    deleteProject: ReturnType<typeof vi.fn>;
    createProject: ReturnType<typeof vi.fn>;
  };
  let notificationServiceMock: { error: ReturnType<typeof vi.fn> };
  let modalServiceMock: { create: ReturnType<typeof vi.fn> };
  let afterClose$: Subject<void>;

  const project = (pid: number, name: string, creationTime: number): DashboardProject => ({
    pid,
    name,
    description: `desc-${pid}`,
    ownerId: 1,
    creationTime,
    color: null,
    accessLevel: "WRITE",
  });

  // A fresh copy every call so the component's in-place sorts don't bleed between tests.
  const projectFixture = (): DashboardProject[] => [project(1, "Alpha", 100), project(2, "Beta", 200)];

  const newProject = project(3, "Gamma", 300);

  const createComponent = (): void => {
    fixture = TestBed.createComponent(UserProjectComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  };

  beforeEach(async () => {
    afterClose$ = new Subject<void>();
    projectServiceMock = {
      getProjectList: vi.fn(() => of(projectFixture())),
      deleteProject: vi.fn(() => of({} as Response)),
      createProject: vi.fn(() => of(newProject)),
    };
    notificationServiceMock = { error: vi.fn() };
    modalServiceMock = { create: vi.fn(() => ({ afterClose: afterClose$ })) };

    await TestBed.configureTestingModule({
      imports: [UserProjectComponent, NoopAnimationsModule],
      providers: [
        { provide: UserProjectService, useValue: projectServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: NzModalService, useValue: modalServiceMock },
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
    })
      // Swap the heavyweight list-item child for the stub above; additive
      // remove/add form (the `set` form has known bugs with standalone imports).
      .overrideComponent(UserProjectComponent, {
        remove: { imports: [UserProjectListItemComponent] },
        add: { imports: [StubUserProjectListItemComponent] },
      })
      .compileComponents();
  });

  afterEach(() => {
    fixture?.destroy();
    document.querySelectorAll(".cdk-overlay-container").forEach(c => (c.innerHTML = ""));
    vi.restoreAllMocks();
  });

  it("reads the current uid from UserService in the constructor", () => {
    createComponent();
    expect(component.uid).toBe(MOCK_USER_ID);
  });

  it("reflects a specific uid returned by getCurrentUser", () => {
    const userService = TestBed.inject(UserService);
    vi.spyOn(userService, "getCurrentUser").mockReturnValue({ ...MOCK_USER, uid: 42 });

    createComponent();

    expect(component.uid).toBe(42);
  });

  it("leaves uid undefined when there is no current user", () => {
    const userService = TestBed.inject(UserService);
    vi.spyOn(userService, "getCurrentUser").mockReturnValue(undefined);

    createComponent();

    expect(component.uid).toBeUndefined();
  });

  it("populates userProjectEntries from getProjectList on ngOnInit", () => {
    createComponent();

    expect(projectServiceMock.getProjectList).toHaveBeenCalledTimes(1);
    expect(component.userProjectEntries.map(p => p.name)).toEqual(["Alpha", "Beta"]);
  });

  it("deleteProject deletes then reloads the list", () => {
    createComponent();

    component.deleteProject(2);

    expect(projectServiceMock.deleteProject).toHaveBeenCalledWith(2);
    // once on init, once from the reload triggered by the delete
    expect(projectServiceMock.getProjectList).toHaveBeenCalledTimes(2);
  });

  it("clickCreateButton flips the create flag on", () => {
    createComponent();
    component.createButtonIsClicked = false;

    component.clickCreateButton();

    expect(component.createButtonIsClicked).toBe(true);
  });

  it("unclickCreateButton resets the flag and clears the pending name", () => {
    createComponent();
    component.createButtonIsClicked = true;
    component.createProjectName = "half-typed";

    component.unclickCreateButton();

    expect(component.createButtonIsClicked).toBe(false);
    expect(component.createProjectName).toBe("");
  });

  it("createNewProject creates and reloads for a valid unique name", () => {
    createComponent();
    component.createProjectName = "Gamma";

    component.createNewProject();

    expect(projectServiceMock.createProject).toHaveBeenCalledTimes(1);
    expect(projectServiceMock.createProject).toHaveBeenCalledWith("Gamma");
    expect(projectServiceMock.getProjectList).toHaveBeenCalledTimes(2);
    expect(notificationServiceMock.error).not.toHaveBeenCalled();
  });

  it("createNewProject rejects an empty name without calling the service", () => {
    createComponent();
    component.createProjectName = "";

    component.createNewProject();

    expect(projectServiceMock.createProject).not.toHaveBeenCalled();
    expect(notificationServiceMock.error).toHaveBeenCalledTimes(1);
  });

  it("createNewProject rejects a duplicate name without calling the service", () => {
    createComponent();
    // "Alpha" is already present from the initial getProjectList fixture.
    component.createProjectName = "Alpha";

    component.createNewProject();

    expect(projectServiceMock.createProject).not.toHaveBeenCalled();
    expect(notificationServiceMock.error).toHaveBeenCalledTimes(1);
  });

  it("sortByCreationTime orders entries ascending by creationTime", () => {
    createComponent();
    component.userProjectEntries = [project(1, "c", 30), project(2, "a", 10), project(3, "b", 20)];

    component.sortByCreationTime();

    expect(component.userProjectEntries.map(p => p.creationTime)).toEqual([10, 20, 30]);
  });

  it("sortByNameAsc orders entries A -> Z case-insensitively", () => {
    createComponent();
    component.userProjectEntries = [project(1, "Banana", 1), project(2, "apple", 2), project(3, "Cherry", 3)];

    component.sortByNameAsc();

    expect(component.userProjectEntries.map(p => p.name)).toEqual(["apple", "Banana", "Cherry"]);
  });

  it("sortByNameDesc orders entries Z -> A case-insensitively", () => {
    createComponent();
    component.userProjectEntries = [project(1, "Banana", 1), project(2, "apple", 2), project(3, "Cherry", 3)];

    component.sortByNameDesc();

    expect(component.userProjectEntries.map(p => p.name)).toEqual(["Cherry", "Banana", "apple"]);
  });

  it("openPublicProject opens the modal with the existing pids disabled and reloads on close", () => {
    createComponent();

    component.openPublicProject();

    expect(modalServiceMock.create).toHaveBeenCalledTimes(1);
    const createArg = modalServiceMock.create.mock.calls[0][0];
    expect(createArg.nzContent).toBe(PublicProjectComponent);
    expect(createArg.nzTitle).toBe("Add Public Projects");
    const disabledList: Set<number> = createArg.nzData.disabledList;
    expect(disabledList).toBeInstanceOf(Set);
    expect(disabledList.has(1)).toBe(true);
    expect(disabledList.has(2)).toBe(true);
    expect(disabledList.size).toBe(2);

    // no reload yet: still just the initial ngOnInit fetch
    expect(projectServiceMock.getProjectList).toHaveBeenCalledTimes(1);

    afterClose$.next();

    expect(projectServiceMock.getProjectList).toHaveBeenCalledTimes(2);
  });
});
