/*
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Component, Input } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ActivatedRoute, UrlSegment } from "@angular/router";
import { of, Subject } from "rxjs";
import { UserProjectSectionComponent } from "./user-project-section.component";
import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DashboardFile } from "../../../../type/dashboard-file.interface";
import { DashboardProject } from "../../../../type/dashboard-project.interface";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import { UserWorkflowComponent } from "../../user-workflow/user-workflow.component";

@Component({
  selector: "texera-saved-workflow-section",
  standalone: true,
  template: "",
})
class StubSavedWorkflowSectionComponent {
  @Input() pid?: number;
  @Input() accessLevel = "READ";
}

@Component({
  selector: "texera-user-workflow",
  standalone: true,
  template: "",
})
class StubUserWorkflowComponent {
  @Input() pid?: number;
  @Input() accessLevel = "READ";
  @Input() updateProjectStatus = "";
}

describe("UserProjectSectionComponent", () => {
  let fixture: ComponentFixture<UserProjectSectionComponent>;
  let component: UserProjectSectionComponent;
  let url$: Subject<UrlSegment[]>;

  let userProjectServiceMock: {
    retrieveProject: ReturnType<typeof vi.fn>;
    getProjectList: ReturnType<typeof vi.fn>;
    refreshFilesOfProject: ReturnType<typeof vi.fn>;
    getProjectFiles: ReturnType<typeof vi.fn>;
    updateProjectColor: ReturnType<typeof vi.fn>;
    deleteProjectColor: ReturnType<typeof vi.fn>;
  };
  let notificationServiceMock: { error: ReturnType<typeof vi.fn> };

  const project = (overrides: Partial<DashboardProject> = {}): DashboardProject => ({
    pid: 42,
    name: "Project 42",
    description: "A project",
    ownerId: 7,
    creationTime: 123456789,
    color: "abcdef",
    accessLevel: "WRITE",
    ...overrides,
  });

  const createComponent = (): void => {
    fixture = TestBed.createComponent(UserProjectSectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  };

  beforeEach(async () => {
    url$ = new Subject<UrlSegment[]>();
    notificationServiceMock = { error: vi.fn() };
    userProjectServiceMock = {
      retrieveProject: vi.fn(() => of(project())),
      getProjectList: vi.fn(() => of([project(), project({ pid: 43, color: null, name: "Project 43" })])),
      refreshFilesOfProject: vi.fn(),
      getProjectFiles: vi.fn(() => [] as DashboardFile[]),
      updateProjectColor: vi.fn(() => of({} as Response)),
      deleteProjectColor: vi.fn(() => of({} as Response)),
    };

    await TestBed.configureTestingModule({
      imports: [UserProjectSectionComponent, StubSavedWorkflowSectionComponent, StubUserWorkflowComponent],
      providers: [
        { provide: UserProjectService, useValue: userProjectServiceMock },
        { provide: NotificationService, useValue: notificationServiceMock },
        { provide: ActivatedRoute, useValue: { url: url$.asObservable() } },
        ...commonTestProviders,
      ],
    })
      .overrideComponent(UserProjectSectionComponent, {
        remove: { imports: [UserWorkflowComponent] },
        add: { imports: [StubUserWorkflowComponent, StubSavedWorkflowSectionComponent] },
      })
      .compileComponents();
  });

  afterEach(() => {
    fixture?.destroy();
    vi.restoreAllMocks();
  });

  it("loads project metadata and files when the route emits a project pid", () => {
    createComponent();

    url$.next([new UrlSegment("user", {}), new UrlSegment("42", {})]);

    expect(userProjectServiceMock.retrieveProject).toHaveBeenCalledWith(42);
    expect(userProjectServiceMock.getProjectList).toHaveBeenCalledTimes(1);
    expect(userProjectServiceMock.refreshFilesOfProject).toHaveBeenCalledWith(42);
    expect(component.pid).toBe(42);
    expect(component.name).toBe("Project 42");
    expect(component.description).toBe("A project");
    expect(component.ownerID).toBe(7);
    expect(component.creationTime).toBe(123456789);
    expect(component.accessLevel).toBe("WRITE");
    expect(component.color).toBe("abcdef");
    expect(component.inputColor).toBe("#abcdef");
    expect(component.colorIsBright).toBe(false);
    expect(component.projectDataIsLoaded).toBe(true);
  });

  it("ignores malformed route segments and does not load a project", () => {
    createComponent();

    url$.next([new UrlSegment("user", {})]);
    url$.next([]);

    expect(userProjectServiceMock.retrieveProject).not.toHaveBeenCalled();
    expect(userProjectServiceMock.getProjectList).not.toHaveBeenCalled();
    expect(userProjectServiceMock.refreshFilesOfProject).not.toHaveBeenCalled();
    expect(component.pid).toBeUndefined();
    expect(component.projectDataIsLoaded).toBe(false);
  });

  it("returns cached files from the service and falls back to an empty list", () => {
    createComponent();
    const files: DashboardFile[] = [{ file: { name: "file-1" } } as DashboardFile];
    userProjectServiceMock.getProjectFiles.mockReturnValue(files);

    expect(component.getUserProjectFilesArray()).toBe(files);

    userProjectServiceMock.getProjectFiles.mockReturnValue(undefined);

    expect(component.getUserProjectFilesArray()).toEqual([]);
  });

  it("rejects invalid color input without calling the service", () => {
    createComponent();

    component.pid = 42;
    component.updateProjectColor("#zzzzzz");

    expect(notificationServiceMock.error).toHaveBeenCalledWith(
      "Cannot update project color. Color must be in valid HEX format"
    );
    expect(userProjectServiceMock.updateProjectColor).not.toHaveBeenCalled();
  });

  it("does not update the color when the value is unchanged", () => {
    createComponent();

    component.pid = 42;
    component.color = "abcdef";
    component.updateProjectColor("#abcdef");

    expect(userProjectServiceMock.updateProjectColor).not.toHaveBeenCalled();
    expect(notificationServiceMock.error).not.toHaveBeenCalled();
  });

  it("updates the project color and recomputes brightness on success", () => {
    createComponent();

    component.pid = 42;
    component.color = "123456";
    userProjectServiceMock.updateProjectColor.mockReturnValue(of({} as Response));

    component.updateProjectColor("#ffffff");

    expect(userProjectServiceMock.updateProjectColor).toHaveBeenCalledWith(42, "ffffff");
    expect(component.color).toBe("ffffff");
    expect(component.colorIsBright).toBe(true);
    expect(component.colorPickerIsSelected).toBe(false);
    expect(component.updateProjectStatus).toBe("updated project color");
  });

  it("reports an error when attempting to remove a missing project color", () => {
    createComponent();

    component.removeProjectColor();

    expect(notificationServiceMock.error).toHaveBeenCalledWith("There is no color to delete for this project");
    expect(userProjectServiceMock.deleteProjectColor).not.toHaveBeenCalled();
  });

  it("removes the project color and resets local state on success", () => {
    createComponent();

    component.pid = 42;
    component.color = "123456";
    component.inputColor = "#123456";
    component.colorPickerIsSelected = true;
    userProjectServiceMock.deleteProjectColor.mockReturnValue(of({} as Response));

    component.removeProjectColor();

    expect(userProjectServiceMock.deleteProjectColor).toHaveBeenCalledWith(42);
    expect(component.color).toBeNull();
    expect(component.inputColor).toBe("#ffffff");
    expect(component.colorPickerIsSelected).toBe(false);
    expect(component.updateProjectStatus).toBe("removed project color");
  });
});
