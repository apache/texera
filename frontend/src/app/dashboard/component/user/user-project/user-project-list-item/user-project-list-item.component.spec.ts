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
import { NzModalService, NzModalRef } from "ng-zorro-antd/modal";
import { provideRouter } from "@angular/router";
import { StubUserService } from "../../../../../common/service/user/stub-user.service";
import { UserService } from "../../../../../common/service/user/user.service";
import { commonTestProviders } from "../../../../../common/testing/test-utils";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { DatePipe } from "@angular/common";
import { of } from "rxjs";
import { MarkdownModule } from "ngx-markdown";

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
  let userProjectService: UserProjectService;
  let modalService: NzModalService;
  let notificationService: NotificationService;
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
      // MarkdownModule.forRoot() backs the <markdown> element in an expanded description.
      imports: [TestHostComponent, HttpClientTestingModule, MarkdownModule.forRoot()],
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
    // Clone so per-test mutations (saveProjectName/Description mutate entry in place)
    // don't leak into other tests through the shared testProject fixture.
    hostFixture.componentInstance.entry = { ...testProject };
    hostFixture.componentInstance.editable = true;
    hostFixture.detectChanges();
    component = hostFixture.componentInstance.inner;
    userProjectService = TestBed.inject(UserProjectService);
    modalService = TestBed.inject(NzModalService);
    notificationService = TestBed.inject(NotificationService);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("should create", () => {
    expect(component).toBeTruthy();
  });

  describe("updateProjectColor", () => {
    it("persists a valid color and updates local state", () => {
      const spy = vi.spyOn(userProjectService, "updateProjectColor").mockReturnValue(of({} as Response));
      component.color = "#123456";

      component.updateProjectColor();

      expect(spy).toHaveBeenCalledWith(1, "123456");
      expect(component.color).toBe("123456");
      expect(component.entry.color).toBe("123456");
      expect(component.editingColor).toBe(false);
    });

    it("rejects an invalid HEX color and does not call the service", () => {
      const spy = vi.spyOn(userProjectService, "updateProjectColor");
      const errorSpy = vi.spyOn(notificationService, "error").mockImplementation(() => {});
      component.color = "#zzz";

      component.updateProjectColor();

      expect(errorSpy).toHaveBeenCalled();
      expect(spy).not.toHaveBeenCalled();
    });

    it("is a no-op when the item is not editable", () => {
      const spy = vi.spyOn(userProjectService, "updateProjectColor");
      component.editable = false;
      component.color = "#123456";

      component.updateProjectColor();

      expect(spy).not.toHaveBeenCalled();
    });
  });

  describe("removeProjectColor", () => {
    it("deletes the color and resets local state", () => {
      const spy = vi.spyOn(userProjectService, "deleteProjectColor").mockReturnValue(of({} as Response));
      // Start from a non-default state so the reset is actually observable.
      component.color = "#123456";
      component.entry = { ...component.entry, color: "123456" };
      component.editingColor = true;

      component.removeProjectColor();

      expect(spy).toHaveBeenCalledWith(1);
      expect(component.color).toBe("#ffffff");
      expect(component.entry.color).toBeNull();
      expect(component.editingColor).toBe(false);
    });
  });

  describe("saveProjectName", () => {
    it("persists a changed name", () => {
      const spy = vi.spyOn(userProjectService, "updateProjectName").mockReturnValue(of({} as Response));

      component.saveProjectName("renamed");

      expect(spy).toHaveBeenCalledWith(1, "renamed");
      expect(component.entry.name).toBe("renamed");
      expect(component.editingName).toBe(false);
    });

    it("does not call the service when the name is unchanged", () => {
      const spy = vi.spyOn(userProjectService, "updateProjectName");

      component.saveProjectName("project1");

      expect(spy).not.toHaveBeenCalled();
      expect(component.editingName).toBe(false);
    });
  });

  describe("saveProjectDescription", () => {
    it("persists a changed description and notifies success", () => {
      const spy = vi.spyOn(userProjectService, "updateProjectDescription").mockReturnValue(of({} as Response));
      const successSpy = vi.spyOn(notificationService, "success").mockImplementation(() => {});

      component.saveProjectDescription("a new description");

      expect(spy).toHaveBeenCalledWith(1, "a new description");
      expect(component.entry.description).toBe("a new description");
      expect(successSpy).toHaveBeenCalled();
      expect(component.editingDescription).toBe(false);
    });

    it("does not call the service when the description is unchanged", () => {
      const spy = vi.spyOn(userProjectService, "updateProjectDescription");

      component.saveProjectDescription("description");

      expect(spy).not.toHaveBeenCalled();
      expect(component.editingDescription).toBe(false);
    });
  });

  describe("onClickOpenShareAccess", () => {
    it("opens the share-access modal and refreshes when it closes", () => {
      const createSpy = vi
        .spyOn(modalService, "create")
        .mockReturnValue({ afterClose: of(undefined) } as unknown as NzModalRef);
      const refreshSpy = vi.spyOn(component.refresh, "emit");

      component.onClickOpenShareAccess();

      expect(createSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          nzContent: ShareAccessComponent,
          nzData: { writeAccess: true, type: "project", id: 1 },
        })
      );
      expect(refreshSpy).toHaveBeenCalled();
    });
  });
  /**
   * The list item decides in its template what a viewer is allowed to touch and how much of a long
   * description to show. The specs above call the save/colour methods directly, so none of the
   * rendered gating had been pinned.
   */
  describe("rendered item", () => {
    /** Re-renders the host with the given entry/editable combination. */
    function render(over: Partial<DashboardProject> = {}, editable = true): HTMLElement {
      hostFixture.componentInstance.entry = { ...testProject, ...over };
      hostFixture.componentInstance.editable = editable;
      hostFixture.detectChanges();
      return hostFixture.nativeElement as HTMLElement;
    }

    it("shows the project name and its creation date", () => {
      const el = render({ name: "quarterly", creationTime: januaryFirst1970 });

      expect(el.textContent).toContain("quarterly");
      // Expected value is formatted here with the same pipe and format string, so the assertion
      // pins both the yyyy-MM-dd HH:mm format and the timestamp it was given, without hard-coding
      // a literal that would only hold in one timezone.
      const expected = new DatePipe("en-US").transform(januaryFirst1970, "yyyy-MM-dd HH:mm");
      expect(el.querySelector("nz-list-item-meta-description p")?.textContent?.trim()).toBe(`Created: ${expected}`);
    });

    it("hides every editing control from a read-only viewer", () => {
      // accessLevel READ reaches this component as editable=false; if the template ignored it the
      // viewer would be shown share and delete buttons for a project they cannot change.
      const el = render({}, false);

      expect(el.querySelector(".edit-name-icon")).toBeNull();
      expect(el.querySelector(".edit-description-icon")).toBeNull();
      expect(el.querySelector("ul[nz-list-item-actions]")).toBeNull();
    });

    it("offers the editing controls to a viewer with write access", () => {
      const el = render({}, true);

      expect(el.querySelector(".edit-name-icon")).not.toBeNull();
      expect(el.querySelector(".edit-description-icon")).not.toBeNull();
      // Share and delete both live in that list; count the buttons rather than just the container
      // (nz-list-item-action renders as an <li>, so the element selector finds nothing).
      expect(el.querySelectorAll("ul[nz-list-item-actions] button").length).toBe(2);
    });

    it("swaps the name for an input once the name is being edited", () => {
      render();
      expect(hostFixture.nativeElement.querySelector("nz-list-item-meta-title input")).toBeNull();

      component.editingName = true;
      hostFixture.detectChanges();

      expect(hostFixture.nativeElement.querySelector("nz-list-item-meta-title input")).not.toBeNull();
    });

    it("starts with the description collapsed and expands it on request", () => {
      // descriptionCollapsed defaults to true, so a list of projects stays compact until the user
      // opens one.
      const el = render({ description: "a long description" });
      expect(el.querySelector(".description-container")).toBeNull();

      component.descriptionCollapsed = false;
      hostFixture.detectChanges();

      expect(hostFixture.nativeElement.querySelector(".description-container")).not.toBeNull();
    });

    it("shows no description block when the description is only whitespace", () => {
      // Expanded, so the trim() guard is the only thing left to hide it: a whitespace-only
      // description would otherwise render an empty expander with nothing in it.
      render({ description: "   " });
      component.descriptionCollapsed = false;
      hostFixture.detectChanges();

      expect(hostFixture.nativeElement.querySelector(".description-container")).toBeNull();
    });

    it("counts the characters typed into the description editor", () => {
      render({ description: "abc" });
      component.editingDescription = true;
      hostFixture.detectChanges();

      const count = hostFixture.nativeElement.querySelector(".character-count")!;
      expect(count.textContent?.trim()).toBe(`3/${component.MAX_PROJECT_DESCRIPTION_CHAR_COUNT}`);

      // It must follow what is in the box, not the value the description started at.
      const textarea = hostFixture.nativeElement.querySelector("textarea")!;
      textarea.value = "abcdef";
      textarea.dispatchEvent(new Event("input"));
      hostFixture.detectChanges();

      expect(hostFixture.nativeElement.querySelector(".character-count")!.textContent?.trim()).toBe(
        `6/${component.MAX_PROJECT_DESCRIPTION_CHAR_COUNT}`
      );
    });

    it("offers the save button only once the description has actually changed", () => {
      render({ description: "abc" });
      component.editingDescription = true;
      hostFixture.detectChanges();
      const textarea = hostFixture.nativeElement.querySelector("textarea")!;

      expect(hostFixture.nativeElement.querySelector(".ant-input-clear-icon")).toBeNull();

      textarea.value = "abcd";
      textarea.dispatchEvent(new Event("input"));
      hostFixture.detectChanges();

      expect(hostFixture.nativeElement.querySelector(".ant-input-clear-icon")).not.toBeNull();
    });
  });
});
