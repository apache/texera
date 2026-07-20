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

import { Component, Input } from "@angular/core";
import { ComponentFixture, TestBed } from "@angular/core/testing";
import { ActivatedRoute } from "@angular/router";
import { BehaviorSubject, of, throwError } from "rxjs";
import { MarkdownComponent } from "ngx-markdown";
import { UserProjectSectionComponent } from "./user-project-section.component";
import { UserWorkflowComponent } from "../../user-workflow/user-workflow.component";
import { UserProjectService } from "../../../../service/user/project/user-project.service";
import { DashboardProject } from "../../../../type/dashboard-project.interface";
import { DashboardFile } from "../../../../type/dashboard-file.interface";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { UserService } from "src/app/common/service/user/user.service";
import { StubUserService } from "src/app/common/service/user/stub-user.service";
import { commonTestProviders } from "src/app/common/testing/test-utils";

// A light HEX (isLightColor -> true) and a dark HEX (isLightColor -> false),
// used to prove the component runs the REAL static color helpers.
const LIGHT_HEX = "ffffff";
const DARK_HEX = "123456";

// Lightweight replacements for the two heavy children (ngx-markdown's
// MarkdownComponent, which needs MarkdownService, and UserWorkflowComponent,
// which pulls the whole workspace DI). They share the real selectors/inputs so
// the template still binds without dragging in the real dependencies.
// eslint-disable-next-line @angular-eslint/component-selector -- must match ngx-markdown's <markdown> element
@Component({ selector: "markdown", standalone: true, template: "" })
class MockMarkdownComponent {
  @Input() data?: string;
}

@Component({ selector: "texera-saved-workflow-section", standalone: true, template: "" })
class MockUserWorkflowComponent {
  @Input() pid?: number;
  @Input() accessLevel?: string;
}

function makeProject(overrides: Partial<DashboardProject> = {}): DashboardProject {
  return {
    pid: 7,
    name: "default project",
    description: "",
    ownerId: 0,
    creationTime: 0,
    color: null,
    accessLevel: "READ",
    ...overrides,
  };
}

const testFile: DashboardFile = {
  ownerEmail: "owner@texera.com",
  accessLevel: "READ",
  file: {
    ownerUid: 1,
    fid: 1,
    size: 10,
    name: "file.csv",
    path: "/file.csv",
    description: "",
    uploadTime: 0,
  },
};

describe("UserProjectSectionComponent", () => {
  let component: UserProjectSectionComponent;
  let fixture: ComponentFixture<UserProjectSectionComponent>;
  let urlSubject: BehaviorSubject<{ path: string }[]>;
  let mockUserProjectService: {
    getProjectList: ReturnType<typeof vi.fn>;
    getProjectFiles: ReturnType<typeof vi.fn>;
    refreshFilesOfProject: ReturnType<typeof vi.fn>;
    retrieveProject: ReturnType<typeof vi.fn>;
    updateProjectColor: ReturnType<typeof vi.fn>;
    deleteProjectColor: ReturnType<typeof vi.fn>;
  };
  let mockNotificationService: { error: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    urlSubject = new BehaviorSubject<{ path: string }[]>([]);
    mockUserProjectService = {
      getProjectList: vi.fn().mockReturnValue(of([])),
      getProjectFiles: vi.fn().mockReturnValue([]),
      refreshFilesOfProject: vi.fn(),
      retrieveProject: vi.fn().mockReturnValue(of(makeProject())),
      updateProjectColor: vi.fn().mockReturnValue(of({} as Response)),
      deleteProjectColor: vi.fn().mockReturnValue(of({} as Response)),
    };
    mockNotificationService = { error: vi.fn() };

    await TestBed.configureTestingModule({
      imports: [UserProjectSectionComponent],
      providers: [
        { provide: UserProjectService, useValue: mockUserProjectService },
        { provide: NotificationService, useValue: mockNotificationService },
        { provide: ActivatedRoute, useValue: { url: urlSubject } },
        { provide: UserService, useClass: StubUserService },
        ...commonTestProviders,
      ],
    })
      // Swap the two heavy children for lightweight stubs so the template renders
      // without the ngx-markdown / workspace DI. Everything else stays real.
      .overrideComponent(UserProjectSectionComponent, {
        remove: { imports: [MarkdownComponent, UserWorkflowComponent] },
        add: { imports: [MockMarkdownComponent, MockUserWorkflowComponent] },
      })
      .compileComponents();

    fixture = TestBed.createComponent(UserProjectSectionComponent);
    component = fixture.componentInstance;
  });

  afterEach(() => {
    fixture?.destroy();
    document.querySelectorAll(".cdk-overlay-container").forEach(c => (c.innerHTML = ""));
    vi.restoreAllMocks();
  });

  it("should create with default (unloaded) state", () => {
    fixture.detectChanges();
    expect(component).toBeTruthy();
    expect(component.pid).toBeUndefined();
    expect(component.projectDataIsLoaded).toBe(false);
  });

  describe("ngOnInit URL handling", () => {
    it("loads project metadata and files when the URL has exactly two segments", () => {
      urlSubject.next([{ path: "projects" }, { path: "7" }]);

      fixture.detectChanges();

      expect(component.pid).toBe(7);
      expect(mockUserProjectService.retrieveProject).toHaveBeenCalledWith(7);
      expect(mockUserProjectService.refreshFilesOfProject).toHaveBeenCalledWith(7);
    });

    it("does nothing when the URL does not have two segments", () => {
      urlSubject.next([{ path: "projects" }]);

      fixture.detectChanges();

      expect(component.pid).toBeUndefined();
      expect(mockUserProjectService.retrieveProject).not.toHaveBeenCalled();
      expect(mockUserProjectService.refreshFilesOfProject).not.toHaveBeenCalled();
      expect(component.projectDataIsLoaded).toBe(false);
    });

    it("does nothing when the second segment path is empty", () => {
      urlSubject.next([{ path: "projects" }, { path: "" }]);

      fixture.detectChanges();

      expect(component.pid).toBeUndefined();
      expect(mockUserProjectService.retrieveProject).not.toHaveBeenCalled();
    });
  });

  describe("getUserProjectFilesArray", () => {
    it("returns an empty array when the service has no files", () => {
      mockUserProjectService.getProjectFiles.mockReturnValue(undefined);
      fixture.detectChanges();

      expect(component.getUserProjectFilesArray()).toEqual([]);
    });

    it("returns the file array from the service", () => {
      const files = [testFile];
      mockUserProjectService.getProjectFiles.mockReturnValue(files);
      fixture.detectChanges();

      const result = component.getUserProjectFilesArray();
      expect(result).toBe(files);
      expect(result.length).toBe(1);
    });
  });

  describe("updateProjectColor", () => {
    it("persists a valid color, marks it bright, and records the status", () => {
      component.pid = 7;
      component.color = null;
      fixture.detectChanges();

      component.updateProjectColor("#" + LIGHT_HEX);

      expect(mockUserProjectService.updateProjectColor).toHaveBeenCalledWith(7, LIGHT_HEX);
      expect(component.color).toBe(LIGHT_HEX);
      // real UserProjectService.isLightColor("ffffff") === true
      expect(component.colorIsBright).toBe(true);
      expect(component.updateProjectStatus).toBe("updated project color");
      expect(component.colorPickerIsSelected).toBe(false);
    });

    it("rejects an invalid HEX color: notifies error and does not call the service", () => {
      component.pid = 7;
      fixture.detectChanges();

      component.updateProjectColor("#zzz");

      expect(mockNotificationService.error).toHaveBeenCalledWith(
        "Cannot update project color. Color must be in valid HEX format"
      );
      expect(mockUserProjectService.updateProjectColor).not.toHaveBeenCalled();
    });

    it("is a no-op when the color is unchanged", () => {
      component.pid = 7;
      component.color = DARK_HEX;
      fixture.detectChanges();

      component.updateProjectColor("#" + DARK_HEX);

      expect(mockUserProjectService.updateProjectColor).not.toHaveBeenCalled();
      expect(mockNotificationService.error).not.toHaveBeenCalled();
    });

    it("is a no-op when the pid is undefined", () => {
      component.pid = undefined;
      component.color = null;
      fixture.detectChanges();

      component.updateProjectColor("#abcdef");

      expect(mockUserProjectService.updateProjectColor).not.toHaveBeenCalled();
    });

    it("notifies the error message when the service call fails", () => {
      component.pid = 7;
      component.color = null;
      mockUserProjectService.updateProjectColor.mockReturnValue(throwError(() => new Error("boom")));
      fixture.detectChanges();

      component.updateProjectColor("#" + DARK_HEX);

      expect(mockNotificationService.error).toHaveBeenCalledWith("boom");
      // failed call must NOT commit the new color
      expect(component.color).toBeNull();
    });
  });

  describe("removeProjectColor", () => {
    it("notifies an error when there is no color to remove", () => {
      component.color = null;
      fixture.detectChanges();

      component.removeProjectColor();

      expect(mockNotificationService.error).toHaveBeenCalledWith("There is no color to delete for this project");
      expect(mockUserProjectService.deleteProjectColor).not.toHaveBeenCalled();
    });

    it("deletes the color and resets local state on success", () => {
      component.pid = 7;
      component.color = DARK_HEX;
      component.inputColor = "#" + DARK_HEX;
      fixture.detectChanges();

      component.removeProjectColor();

      expect(mockUserProjectService.deleteProjectColor).toHaveBeenCalledWith(7);
      expect(component.color).toBeNull();
      expect(component.inputColor).toBe("#ffffff");
      expect(component.updateProjectStatus).toBe("removed project color");
      expect(component.colorPickerIsSelected).toBe(false);
    });
  });

  describe("getUserProjectMetadata (via ngOnInit)", () => {
    it("populates fields from retrieveProject and applies the dark color", () => {
      mockUserProjectService.retrieveProject.mockReturnValue(
        of(makeProject({ pid: 7, name: "Retrieved", ownerId: 3, creationTime: 999, color: DARK_HEX }))
      );
      mockUserProjectService.getProjectList.mockReturnValue(of([]));
      urlSubject.next([{ path: "projects" }, { path: "7" }]);

      fixture.detectChanges();

      expect(component.name).toBe("Retrieved");
      expect(component.ownerID).toBe(3);
      expect(component.creationTime).toBe(999);
      expect(component.color).toBe(DARK_HEX);
      expect(component.inputColor).toBe("#" + DARK_HEX);
      // real UserProjectService.isLightColor("123456") === false
      expect(component.colorIsBright).toBe(false);
      expect(component.projectDataIsLoaded).toBe(true);
    });

    it("overwrites single-project fields from getProjectList when the pid matches", () => {
      mockUserProjectService.retrieveProject.mockReturnValue(
        of(makeProject({ pid: 7, name: "FromRetrieve", ownerId: 1, creationTime: 111, color: null }))
      );
      mockUserProjectService.getProjectList.mockReturnValue(
        of([
          makeProject({
            pid: 7,
            name: "FromList",
            description: "list description",
            ownerId: 42,
            creationTime: 12345,
            color: LIGHT_HEX,
            accessLevel: "WRITE",
          }),
          makeProject({ pid: 8, name: "Other" }),
        ])
      );
      urlSubject.next([{ path: "projects" }, { path: "7" }]);

      fixture.detectChanges();

      // getProjectList runs after retrieveProject, so its matching-pid values win
      expect(component.name).toBe("FromList");
      expect(component.description).toBe("list description");
      expect(component.ownerID).toBe(42);
      expect(component.creationTime).toBe(12345);
      expect(component.accessLevel).toBe("WRITE");
      expect(component.color).toBe(LIGHT_HEX);
      expect(component.inputColor).toBe("#" + LIGHT_HEX);
      expect(component.colorIsBright).toBe(true);
      expect(component.projectDataIsLoaded).toBe(true);

      // the color tag div is rendered because color !== null
      const el = fixture.nativeElement as HTMLElement;
      expect(el.querySelector("#left-div")).not.toBeNull();
      expect(el.textContent).toContain("FromList");
    });

    it("leaves single-project fields untouched when getProjectList is empty", () => {
      mockUserProjectService.retrieveProject.mockReturnValue(
        of(makeProject({ pid: 7, name: "OnlyRetrieve", color: null }))
      );
      mockUserProjectService.getProjectList.mockReturnValue(of([]));
      urlSubject.next([{ path: "projects" }, { path: "7" }]);

      fixture.detectChanges();

      expect(component.name).toBe("OnlyRetrieve");
      expect(component.description).toBe("");
      expect(component.accessLevel).toBe("READ");
      expect(component.projectDataIsLoaded).toBe(true);
    });
  });
});
