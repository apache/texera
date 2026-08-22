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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
import { HttpErrorResponse } from "@angular/common/http";
import { of, Subject, throwError } from "rxjs";

import { NZ_MODAL_DATA, NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { NzMessageService } from "ng-zorro-antd/message";

import { ShareAccessComponent } from "./share-access.component";
import { ShareAccessService } from "../../../service/user/share-access/share-access.service";
import { UserService } from "../../../../common/service/user/user.service";
import { GmailService } from "../../../../common/service/gmail/gmail.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import { WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { Privilege } from "../../../type/share-access.interface";

interface SetupOptions {
  type?: string;
  id?: number;
  inWorkspace?: boolean;
  currentEmail?: string | undefined;
}

describe("ShareAccessComponent", () => {
  let gmailSpy: { sendEmail: ReturnType<typeof vi.fn> };
  let accessServiceSpy: {
    grantAccess: ReturnType<typeof vi.fn>;
    getAccessList: ReturnType<typeof vi.fn>;
    getOwner: ReturnType<typeof vi.fn>;
    revokeAccess: ReturnType<typeof vi.fn>;
  };
  let notificationSpy: { success: ReturnType<typeof vi.fn>; error: ReturnType<typeof vi.fn> };
  let messageSpy: { error: ReturnType<typeof vi.fn> };
  let modalRefSpy: { close: ReturnType<typeof vi.fn> };
  let modalServiceSpy: { create: ReturnType<typeof vi.fn> };
  let workflowPersistSpy: {
    getWorkflowIsPublished: ReturnType<typeof vi.fn>;
    updateWorkflowIsPublished: ReturnType<typeof vi.fn>;
    getPublishStatus: ReturnType<typeof vi.fn>;
    getWorkflowPersistedStream: ReturnType<typeof vi.fn>;
    isWorkflowPersistEnabled: ReturnType<typeof vi.fn>;
    persistWorkflow: ReturnType<typeof vi.fn>;
    pinLatestVersion: ReturnType<typeof vi.fn>;
    unpinVersion: ReturnType<typeof vi.fn>;
  };
  let datasetServiceSpy: {
    getDataset: ReturnType<typeof vi.fn>;
    updateDatasetPublicity: ReturnType<typeof vi.fn>;
  };
  let workflowActionSpy: {
    setWorkflowIsPublished: ReturnType<typeof vi.fn>;
    getWorkflow: ReturnType<typeof vi.fn>;
  };
  let userServiceCurrentEmail: string | undefined;
  let capturedModalConfigs: any[];
  let lastFixture: ComponentFixture<ShareAccessComponent> | undefined;
  /** Stands in for the editor's autosave landing, which is what the line has to follow. */
  let persisted: Subject<any>;

  function setupComponent(opts: SetupOptions = {}): ShareAccessComponent {
    const { type = "workflow", id = 1, inWorkspace = false, currentEmail = "me@example.com" } = opts;
    userServiceCurrentEmail = currentEmail;

    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule, NoopAnimationsModule, ShareAccessComponent],
      providers: [
        { provide: NZ_MODAL_DATA, useValue: { type, id, allOwners: [], inWorkspace } },
        { provide: ShareAccessService, useValue: accessServiceSpy },
        {
          provide: UserService,
          useValue: {
            getCurrentUser: () => (userServiceCurrentEmail ? { email: userServiceCurrentEmail } : undefined),
          },
        },
        { provide: GmailService, useValue: gmailSpy },
        { provide: NotificationService, useValue: notificationSpy },
        { provide: NzMessageService, useValue: messageSpy },
        { provide: NzModalService, useValue: modalServiceSpy },
        { provide: NzModalRef, useValue: modalRefSpy },
        { provide: WorkflowPersistService, useValue: workflowPersistSpy },
        { provide: DatasetService, useValue: datasetServiceSpy },
        { provide: WorkflowActionService, useValue: workflowActionSpy },
      ],
    });
    const fixture = TestBed.createComponent(ShareAccessComponent);
    fixture.detectChanges();
    lastFixture = fixture;
    return fixture.componentInstance;
  }

  /** Everything the publish panel renders, for the assertions about what the author actually reads. */
  function publishLineText(): string {
    lastFixture!.detectChanges();
    return (lastFixture!.nativeElement.querySelector(".publish-anchor")?.textContent ?? "").replace(/\s+/g, " ").trim();
  }

  /**
   * The two sides of the switch. Which one is highlighted is the control's own business -- it paints
   * the selection from an animation frame, which this environment never runs -- so these assert the
   * labels and leave the state itself to `publishState`.
   */
  function segments(): string[] {
    lastFixture!.detectChanges();
    return Array.from(lastFixture!.nativeElement.querySelectorAll(".ant-segmented-item-label")).map((b: any) =>
      b.textContent.replace(/\s+/g, " ").trim()
    );
  }

  /** The card under the line, which only a pin holding edits back puts on screen. */
  function publishNoteText(): string {
    lastFixture!.detectChanges();
    return (lastFixture!.nativeElement.querySelector(".publish-card")?.textContent ?? "").replace(/\s+/g, " ").trim();
  }

  beforeEach(() => {
    TestBed.resetTestingModule();
    persisted = new Subject<any>();
    capturedModalConfigs = [];
    gmailSpy = { sendEmail: vi.fn() };
    accessServiceSpy = {
      grantAccess: vi.fn().mockReturnValue(of(null)),
      getAccessList: vi.fn().mockReturnValue(of([])),
      getOwner: vi.fn().mockReturnValue(of("owner@example.com")),
      revokeAccess: vi.fn().mockReturnValue(of(null)),
    };
    notificationSpy = { success: vi.fn(), error: vi.fn() };
    messageSpy = { error: vi.fn() };
    modalRefSpy = { close: vi.fn() };
    modalServiceSpy = {
      create: vi.fn().mockImplementation((config: any) => {
        capturedModalConfigs.push(config);
        return { close: vi.fn() };
      }),
    };
    const following = { isPublished: true, isPinned: false, hasUnpublishedChanges: false };
    workflowPersistSpy = {
      getWorkflowIsPublished: vi.fn().mockReturnValue(of("Private")),
      updateWorkflowIsPublished: vi.fn().mockReturnValue(of(null)),
      getPublishStatus: vi.fn().mockReturnValue(of(following)),
      getWorkflowPersistedStream: vi.fn().mockReturnValue(persisted.asObservable()),
      isWorkflowPersistEnabled: vi.fn().mockReturnValue(true),
      persistWorkflow: vi.fn().mockReturnValue(of({ wid: 3 })),
      pinLatestVersion: vi.fn().mockReturnValue(of({ ...following, isPinned: true })),
      unpinVersion: vi.fn().mockReturnValue(of(following)),
    };
    datasetServiceSpy = {
      getDataset: vi.fn().mockReturnValue(of({ dataset: { isPublic: false } })),
      updateDatasetPublicity: vi.fn().mockReturnValue(of(null)),
    };
    workflowActionSpy = {
      setWorkflowIsPublished: vi.fn(),
      getWorkflow: vi.fn().mockReturnValue({ wid: 3, name: "w", content: { operators: [], links: [] } }),
    };
  });

  function getFooterButton(config: any, label: string): { onClick: () => void } {
    return config.nzFooter.find((b: any) => b.label === label);
  }

  describe("ngOnInit", () => {
    it("loads access list and owner from ShareAccessService", () => {
      const accessList = [{ email: "a@example.com", name: "A", privilege: Privilege.READ }];
      accessServiceSpy.getAccessList.mockReturnValue(of(accessList));
      accessServiceSpy.getOwner.mockReturnValue(of("owner@example.com"));
      const c = setupComponent({ type: "workflow", id: 7 });
      expect(accessServiceSpy.getAccessList).toHaveBeenCalledWith("workflow", 7);
      expect(accessServiceSpy.getOwner).toHaveBeenCalledWith("workflow", 7);
      expect(c.accessList).toEqual(accessList);
      expect(c.owner).toBe("owner@example.com");
    });

    it("loads publish state for workflow via WorkflowPersistService", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      const c = setupComponent({ type: "workflow", id: 9 });
      expect(workflowPersistSpy.getWorkflowIsPublished).toHaveBeenCalledWith(9);
      expect(c.isPublic).toBe(true);
    });

    it("sets isPublic to false when workflow publish state is Private", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const c = setupComponent({ type: "workflow" });
      expect(c.isPublic).toBe(false);
    });

    it("loads publish state for dataset via DatasetService.getDataset", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: true } }));
      const c = setupComponent({ type: "dataset", id: 12 });
      expect(datasetServiceSpy.getDataset).toHaveBeenCalledWith(12);
      expect(c.isPublic).toBe(true);
    });

    it("does not query publish state for non-workflow/dataset types", () => {
      setupComponent({ type: "project", id: 4 });
      expect(workflowPersistSpy.getWorkflowIsPublished).not.toHaveBeenCalled();
      expect(datasetServiceSpy.getDataset).not.toHaveBeenCalled();
    });
  });

  describe("publish state line", () => {
    // The owner of the workflow, so hasWriteAccess is true.
    const asOwner = { currentEmail: "owner@example.com" };

    it("does not fetch publish status for an unpublished workflow", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });
      expect(workflowPersistSpy.getPublishStatus).not.toHaveBeenCalled();
      expect(c.publishStatus).toBeUndefined();
    });

    it("reports unpublished changes on a pinned workflow", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: true })
      );
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });
      expect(workflowPersistSpy.getPublishStatus).toHaveBeenCalledWith(3);
      expect(c.publishStatus?.hasUnpublishedChanges).toBe(true);
    });

    it("does not fetch publish status for a viewer who cannot publish", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      accessServiceSpy.getAccessList.mockReturnValue(
        of([{ email: "reader@example.com", name: "R", privilege: Privilege.READ }])
      );
      const c = setupComponent({ type: "workflow", id: 3, currentEmail: "reader@example.com" });
      expect(workflowPersistSpy.getPublishStatus).not.toHaveBeenCalled();
      expect(c.publishStatus).toBeUndefined();
    });

    it("clears the pending state after moving the pin forward", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: true })
      );
      workflowPersistSpy.pinLatestVersion.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: false })
      );
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });

      c.choosePublicCopy(true, true);

      expect(workflowPersistSpy.pinLatestVersion).toHaveBeenCalledWith(3);
      expect(c.publishStatus?.hasUnpublishedChanges).toBe(false);
      expect(c.isPinning).toBe(false);
    });

    it("goes back to following when the pin is dropped", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: true })
      );
      workflowPersistSpy.unpinVersion.mockReturnValue(
        of({ isPublished: true, isPinned: false, hasUnpublishedChanges: false })
      );
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });

      c.choosePublicCopy(false);

      expect(workflowPersistSpy.unpinVersion).toHaveBeenCalledWith(3);
      expect(c.publishStatus?.isPinned).toBe(false);
      // Nothing is held back once the public follows the latest.
      expect(c.publishStatus?.hasUnpublishedChanges).toBe(false);
      expect(c.isPinning).toBe(false);
    });

    it("names the pinned version by its date once the author has moved on", () => {
      // The date is how the same version is named in the revision panel, which is where the author
      // goes to restore it. Naming it any other way would leave them matching two descriptions.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({
          isPublished: true,
          isPinned: true,
          hasUnpublishedChanges: true,
          pinnedVersionTime: Date.parse("2026-08-12T23:59:00"),
        })
      );
      setupComponent({ type: "workflow", id: 3, ...asOwner });

      expect(publishLineText()).toContain("Pinned to Aug 12, 23:59:00");
      expect(publishLineText()).toContain("Your later edits stay private");
    });

    it("says the pinned copy is the current one rather than dating it", () => {
      // A date here would invite the author to work out whether it is still what they have; saying
      // so is the answer to the question they would be asking.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({
          isPublished: true,
          isPinned: true,
          hasUnpublishedChanges: false,
          pinnedVersionTime: Date.parse("2026-08-12T23:59:00"),
        })
      );
      setupComponent({ type: "workflow", id: 3, ...asOwner });

      expect(publishLineText()).toContain("your current version");
      expect(publishLineText()).not.toContain("Aug 12");
    });

    it("prints the pinned time to the second, as the revision panel does", () => {
      // The line names the version the revision panel marks, so the two must print it identically --
      // one format, never varied, is what guarantees that.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({
          isPublished: true,
          isPinned: true,
          hasUnpublishedChanges: true,
          pinnedVersionTime: Date.parse("2026-08-12T23:58:51"),
        })
      );
      setupComponent({ type: "workflow", id: 3, ...asOwner });

      expect(publishLineText()).toContain("Aug 12, 23:58:51");
    });

    it("says the same thing about both states, with only the answer changing", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: false, hasUnpublishedChanges: false })
      );
      setupComponent({ type: "workflow", id: 3, ...asOwner });

      expect(publishLineText()).toContain("The public sees your latest");
      expect(segments()).toEqual(["Follow latest", "Pinned"]);
    });

    it("drops the line when the workflow is unpublished", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });
      expect(c.publishStatus).toBeDefined();

      c.unpublishWorkflow();

      expect(c.isPublic).toBe(false);
      expect(c.publishStatus).toBeUndefined();
    });

    it("hides the notice when the status request fails", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(throwError(() => new Error("boom")));
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });
      expect(c.publishStatus).toBeUndefined();
    });

    it("surfaces a failed pin and stops the spinner", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: true })
      );
      workflowPersistSpy.pinLatestVersion.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "nope" }, status: 500 }))
      );
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });

      c.choosePublicCopy(true, true);

      expect(notificationSpy.error).toHaveBeenCalledWith("nope");
      expect(c.isPinning).toBe(false);
      // The pending state stands, so the author can try again.
      expect(c.publishStatus?.hasUnpublishedChanges).toBe(true);
    });

    it("never asks for publish status on a dataset", () => {
      const c = setupComponent({ type: "dataset", id: 12, ...asOwner });
      expect(workflowPersistSpy.getPublishStatus).not.toHaveBeenCalled();
      expect(c.publishStatus).toBeUndefined();
    });

    it("fetches the status after publishing from private", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });
      expect(workflowPersistSpy.getPublishStatus).not.toHaveBeenCalled();

      c.publishWorkflow();

      expect(workflowPersistSpy.updateWorkflowIsPublished).toHaveBeenCalledWith(3, true);
      expect(workflowPersistSpy.getPublishStatus).toHaveBeenCalledWith(3);
      expect(c.publishStatus).toBeDefined();
    });

    it("offers both choices, with the state itself naming the one in force", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: false })
      );
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });

      expect(segments()).toEqual(["Follow latest", "Pinned"]);
      expect(c.publishState).toBe("pinned");
    });

    it("does nothing when the side already in force is picked again", () => {
      // Re-picking "Pinned" would otherwise publish whatever has been edited since -- which is the
      // card's job, and the card says what it would publish.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: true })
      );
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });

      c.choosePublicCopy(true);

      expect(workflowPersistSpy.pinLatestVersion).not.toHaveBeenCalled();
      expect(c.publishStatus?.hasUnpublishedChanges).toBe(true);
    });

    it("does nothing when following is picked while already following", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: false, hasUnpublishedChanges: false })
      );
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });

      c.choosePublicCopy(false);

      expect(workflowPersistSpy.unpinVersion).not.toHaveBeenCalled();
    });

    it("names the pinned version and the way out while it is holding edits back", () => {
      // The card is the answer to "my edits are not public": which version is out there, what it
      // costs, and the act that ends it. In every other state there is nothing to decide.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({
          isPublished: true,
          isPinned: true,
          hasUnpublishedChanges: true,
          pinnedVersionTime: Date.parse("2026-08-12T23:59:00"),
        })
      );
      setupComponent({ type: "workflow", id: 3, ...asOwner });

      expect(publishNoteText()).toContain("Pinned to Aug 12, 23:59:00");
      expect(publishNoteText()).toContain("Your later edits stay private");
      expect(publishNoteText()).toContain("Update to current");
    });

    it("points at the version panel once the pinned copy is what the author has", () => {
      // The recovery path when they do move on, named where they are already looking.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: false })
      );
      setupComponent({ type: "workflow", id: 3, ...asOwner });

      expect(publishLineText()).toContain("Currently public");
    });

    it("saves what the editor is holding before asking about it", () => {
      // The editor saves a few seconds after the last edit, so a dialog opened inside that window
      // would otherwise report on the canvas as it was before those edits.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: false })
      );
      setupComponent({ type: "workflow", id: 3, inWorkspace: true, ...asOwner });

      expect(workflowPersistSpy.persistWorkflow).toHaveBeenCalled();
    });

    it("does not write anything back while a version is being previewed", () => {
      // Previewing turns persistence off; saving here would write the preview back as the author's
      // own work.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.isWorkflowPersistEnabled.mockReturnValue(false);
      setupComponent({ type: "workflow", id: 3, inWorkspace: true, ...asOwner });

      expect(workflowPersistSpy.persistWorkflow).not.toHaveBeenCalled();
    });

    it("has nothing to flush when opened outside the workspace", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      setupComponent({ type: "workflow", id: 3, ...asOwner });

      expect(workflowPersistSpy.persistWorkflow).not.toHaveBeenCalled();
    });

    it("re-reads the state when a save lands", () => {
      // The editor saves on a debounce, so the answer the dialog fetched when it opened describes a
      // workflow the author may already have moved past. Nothing else tells it -- the canvas changing
      // is not the save landing, and until the save lands the server still answers with the old copy.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: false })
      );
      setupComponent({ type: "workflow", id: 3, ...asOwner });
      expect(publishNoteText()).toBe("");

      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: true })
      );
      persisted.next({ wid: 3 });

      expect(workflowPersistSpy.getPublishStatus).toHaveBeenCalledTimes(2);
      expect(publishNoteText()).toContain("Update to current");
    });

    it("says nothing extra when the pinned copy is what the author has", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: false })
      );
      setupComponent({ type: "workflow", id: 3, ...asOwner });

      expect(publishNoteText()).toBe("");
    });

    it("publishes the edits a pin was holding back, from the note", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.getPublishStatus.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: true })
      );
      workflowPersistSpy.pinLatestVersion.mockReturnValue(
        of({ isPublished: true, isPinned: true, hasUnpublishedChanges: false })
      );
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });

      lastFixture!.nativeElement.querySelector(".publish-card-action").click();

      expect(workflowPersistSpy.pinLatestVersion).toHaveBeenCalledWith(3);
      expect(c.publishStatus?.hasUnpublishedChanges).toBe(false);
      // Answered, so the question goes away.
      expect(publishNoteText()).toBe("");
    });

    it("says nothing about the copies before the status arrives", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const c = setupComponent({ type: "workflow", id: 3, ...asOwner });
      expect(c.publishStatus).toBeUndefined();
      expect(publishLineText()).toBe("");
    });
  });

  describe("handleInputConfirm", () => {
    it("splits input on whitespace, commas, and semicolons into emailTags", () => {
      const c = setupComponent();
      c.validateForm.get("email")?.setValue("a@example.com, b@example.com;c@example.com d@example.com");
      c.handleInputConfirm();
      expect(c.emailTags).toEqual(["a@example.com", "b@example.com", "c@example.com", "d@example.com"]);
    });

    it("rejects invalid emails via NzMessageService.error", () => {
      const c = setupComponent();
      c.validateForm.get("email")?.setValue("not-an-email");
      c.handleInputConfirm();
      expect(messageSpy.error).toHaveBeenCalledWith("not-an-email is not a valid email");
      expect(c.emailTags).toEqual([]);
    });

    it("rejects duplicate emails via NzMessageService.error", () => {
      const c = setupComponent();
      c.emailTags = ["dup@example.com"];
      c.validateForm.get("email")?.setValue("dup@example.com");
      c.handleInputConfirm();
      expect(messageSpy.error).toHaveBeenCalledWith("dup@example.com is already in the tags");
      expect(c.emailTags).toEqual(["dup@example.com"]);
    });

    it("resets the email form control after processing", () => {
      const c = setupComponent();
      c.validateForm.get("email")?.setValue("ok@example.com");
      c.handleInputConfirm();
      expect(c.validateForm.get("email")?.value).toBeNull();
    });

    it("calls event.preventDefault when an event is provided", () => {
      const c = setupComponent();
      const event = { preventDefault: vi.fn() } as unknown as Event;
      c.handleInputConfirm(event);
      expect(event.preventDefault).toHaveBeenCalled();
    });
  });

  describe("onPaste", () => {
    it("concatenates clipboard text to the existing email value and runs handleInputConfirm", () => {
      const c = setupComponent();
      c.validateForm.get("email")?.setValue("first@example.com,");
      const event = {
        preventDefault: vi.fn(),
        clipboardData: { getData: vi.fn().mockReturnValue("second@example.com") },
      } as unknown as ClipboardEvent;
      c.onPaste(event);
      expect(event.preventDefault).toHaveBeenCalled();
      expect(c.emailTags).toEqual(["first@example.com", "second@example.com"]);
    });

    it("is a no-op when clipboard data is empty", () => {
      const c = setupComponent();
      const event = {
        preventDefault: vi.fn(),
        clipboardData: { getData: vi.fn().mockReturnValue("") },
      } as unknown as ClipboardEvent;
      c.onPaste(event);
      expect(c.emailTags).toEqual([]);
    });
  });

  describe("grantAccess", () => {
    function grantAndCaptureMessage(c: ShareAccessComponent): string {
      c.emailTags = ["to@example.com"];
      c.grantAccess();
      return gmailSpy.sendEmail.mock.calls[0][1] as string;
    }

    it("uses the workflow dashboard path when sharing a workflow", () => {
      const message = grantAndCaptureMessage(setupComponent({ type: "workflow", id: 11 }));
      expect(message).toContain("/user/workflow/11");
    });

    it("uses the dataset dashboard path when sharing a dataset", () => {
      const message = grantAndCaptureMessage(setupComponent({ type: "dataset", id: 22 }));
      expect(message).toContain("/user/dataset/22");
    });

    it("uses the project dashboard path when sharing a project", () => {
      const message = grantAndCaptureMessage(setupComponent({ type: "project", id: 33 }));
      expect(message).toContain("/user/project/33");
    });

    it("omits the access URL when sharing a computing-unit", () => {
      const message = grantAndCaptureMessage(setupComponent({ type: "computing-unit", id: 44 }));
      expect(message).not.toContain("/user/");
    });

    it("calls ShareAccessService.grantAccess with the selected access level for each tag", () => {
      const c = setupComponent({ type: "workflow", id: 5 });
      c.validateForm.get("accessLevel")?.setValue("READ");
      c.emailTags = ["a@example.com", "b@example.com"];
      c.grantAccess();
      expect(accessServiceSpy.grantAccess).toHaveBeenCalledWith("workflow", 5, "a@example.com", "READ");
      expect(accessServiceSpy.grantAccess).toHaveBeenCalledWith("workflow", 5, "b@example.com", "READ");
    });

    it("shows a success notification and clears emailTags after granting", () => {
      const c = setupComponent({ type: "workflow", id: 5 });
      c.emailTags = ["x@example.com"];
      c.grantAccess();
      expect(notificationSpy.success).toHaveBeenCalledWith("workflow shared with x@example.com successfully.");
      expect(c.emailTags).toEqual([]);
    });

    it("surfaces HttpErrorResponse via NotificationService.error", () => {
      accessServiceSpy.grantAccess.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "boom" }, status: 500 }))
      );
      const c = setupComponent();
      c.emailTags = ["x@example.com"];
      c.grantAccess();
      expect(notificationSpy.error).toHaveBeenCalledWith("boom");
    });
  });

  describe("hasWriteAccess", () => {
    it("returns false when there is no current user email", () => {
      const c = setupComponent({ currentEmail: undefined });
      expect(c.hasWriteAccess).toBe(false);
    });

    it("returns true when the current user is the owner", () => {
      accessServiceSpy.getOwner.mockReturnValue(of("me@example.com"));
      const c = setupComponent({ currentEmail: "me@example.com" });
      expect(c.hasWriteAccess).toBe(true);
    });

    it("returns true when the current user has WRITE privilege in the access list", () => {
      accessServiceSpy.getAccessList.mockReturnValue(
        of([{ email: "me@example.com", name: "Me", privilege: Privilege.WRITE }])
      );
      const c = setupComponent({ currentEmail: "me@example.com" });
      expect(c.hasWriteAccess).toBe(true);
    });

    it("returns false when the current user has READ privilege", () => {
      accessServiceSpy.getAccessList.mockReturnValue(
        of([{ email: "me@example.com", name: "Me", privilege: Privilege.READ }])
      );
      const c = setupComponent({ currentEmail: "me@example.com" });
      expect(c.hasWriteAccess).toBe(false);
    });
  });

  describe("verifyRevokeAccess / revokeAccess", () => {
    it("opens a self-revoke modal when revoking own access", () => {
      const c = setupComponent({ currentEmail: "me@example.com", type: "workflow" });
      c.verifyRevokeAccess("me@example.com");
      const config = capturedModalConfigs[0];
      expect(config.nzTitle).toBe("Revoke Your Access");
      expect(config.nzContent).toContain("your own access");
    });

    it("opens an other-user revoke modal when revoking someone else", () => {
      const c = setupComponent({ currentEmail: "me@example.com", type: "workflow" });
      c.verifyRevokeAccess("other@example.com");
      const config = capturedModalConfigs[0];
      expect(config.nzTitle).toBe("Revoke Access");
      expect(config.nzContent).toContain("other@example.com");
    });

    it("calls revokeAccess on confirm and emits refresh on destroy for self-revoke", () => {
      const c = setupComponent({ currentEmail: "me@example.com" });
      const refreshSpy = vi.fn();
      c.refresh.subscribe(refreshSpy);
      c.verifyRevokeAccess("me@example.com");
      getFooterButton(capturedModalConfigs[0], "Revoke").onClick();
      expect(accessServiceSpy.revokeAccess).toHaveBeenCalledWith("workflow", 1, "me@example.com");
      expect(modalRefSpy.close).toHaveBeenCalledWith({ userRevokedOwnAccess: true });
      c.ngOnDestroy();
      expect(refreshSpy).toHaveBeenCalled();
    });

    it("does not close the outer modal when revoking another user", () => {
      const c = setupComponent({ currentEmail: "me@example.com" });
      c.verifyRevokeAccess("other@example.com");
      getFooterButton(capturedModalConfigs[0], "Revoke").onClick();
      expect(accessServiceSpy.revokeAccess).toHaveBeenCalledWith("workflow", 1, "other@example.com");
      expect(modalRefSpy.close).not.toHaveBeenCalled();
    });

    it("surfaces revoke HttpErrorResponse via NotificationService.error", () => {
      accessServiceSpy.revokeAccess.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "nope" }, status: 403 }))
      );
      const c = setupComponent({ currentEmail: "me@example.com" });
      c.verifyRevokeAccess("other@example.com");
      getFooterButton(capturedModalConfigs[0], "Revoke").onClick();
      expect(notificationSpy.error).toHaveBeenCalledWith("nope");
    });
  });

  describe("changeAccessLevel", () => {
    it("calls applyAccessLevelChange directly when not a self-downgrade", () => {
      const c = setupComponent({ currentEmail: "me@example.com", type: "workflow", id: 3 });
      accessServiceSpy.grantAccess.mockClear();
      c.changeAccessLevel("other@example.com", "READ");
      expect(modalServiceSpy.create).not.toHaveBeenCalled();
      expect(accessServiceSpy.grantAccess).toHaveBeenCalledWith("workflow", 3, "other@example.com", "READ");
    });

    it("opens a downgrade-confirmation modal when downgrading own WRITE access to READ", () => {
      accessServiceSpy.getAccessList.mockReturnValue(
        of([{ email: "me@example.com", name: "Me", privilege: Privilege.WRITE }])
      );
      const c = setupComponent({ currentEmail: "me@example.com", type: "workflow", id: 3 });
      accessServiceSpy.grantAccess.mockClear();
      c.changeAccessLevel("me@example.com", "READ");
      expect(modalServiceSpy.create).toHaveBeenCalled();
      expect(capturedModalConfigs[0].nzTitle).toBe("Downgrade Your Access");
      expect(accessServiceSpy.grantAccess).not.toHaveBeenCalled();
      getFooterButton(capturedModalConfigs[0], "Confirm").onClick();
      expect(accessServiceSpy.grantAccess).toHaveBeenCalledWith("workflow", 3, "me@example.com", "READ");
    });

    it("does not open the downgrade modal when upgrading own access from READ to WRITE", () => {
      accessServiceSpy.getAccessList.mockReturnValue(
        of([{ email: "me@example.com", name: "Me", privilege: Privilege.READ }])
      );
      const c = setupComponent({ currentEmail: "me@example.com" });
      accessServiceSpy.grantAccess.mockClear();
      c.changeAccessLevel("me@example.com", "WRITE");
      expect(modalServiceSpy.create).not.toHaveBeenCalled();
      expect(accessServiceSpy.grantAccess).toHaveBeenCalled();
    });
  });

  describe("verifyPublish / verifyUnpublish", () => {
    it("publishes a workflow on confirm and updates the action service when inWorkspace", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const c = setupComponent({ type: "workflow", id: 8, inWorkspace: true });
      c.verifyPublish();
      getFooterButton(capturedModalConfigs[0], "Publish").onClick();
      expect(workflowPersistSpy.updateWorkflowIsPublished).toHaveBeenCalledWith(8, true);
      expect(workflowActionSpy.setWorkflowIsPublished).toHaveBeenCalledWith(1);
    });

    it("does not call WorkflowActionService.setWorkflowIsPublished when not inWorkspace", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const c = setupComponent({ type: "workflow", id: 8, inWorkspace: false });
      c.verifyPublish();
      getFooterButton(capturedModalConfigs[0], "Publish").onClick();
      expect(workflowActionSpy.setWorkflowIsPublished).not.toHaveBeenCalled();
    });

    it("publishes a dataset on confirm", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: false } }));
      const c = setupComponent({ type: "dataset", id: 9 });
      c.verifyPublish();
      getFooterButton(capturedModalConfigs[0], "Publish").onClick();
      expect(datasetServiceSpy.updateDatasetPublicity).toHaveBeenCalledWith(9);
    });

    it("does not open the publish modal when the item is already public", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      const c = setupComponent({ type: "workflow" });
      c.verifyPublish();
      expect(modalServiceSpy.create).not.toHaveBeenCalled();
    });

    it("unpublishes a workflow on confirm and updates the action service when inWorkspace", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      const c = setupComponent({ type: "workflow", id: 8, inWorkspace: true });
      c.verifyUnpublish();
      getFooterButton(capturedModalConfigs[0], "Unpublish").onClick();
      expect(workflowPersistSpy.updateWorkflowIsPublished).toHaveBeenCalledWith(8, false);
      expect(workflowActionSpy.setWorkflowIsPublished).toHaveBeenCalledWith(0);
    });

    it("unpublishes a dataset on confirm", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: true } }));
      const c = setupComponent({ type: "dataset", id: 9 });
      c.verifyUnpublish();
      getFooterButton(capturedModalConfigs[0], "Unpublish").onClick();
      expect(datasetServiceSpy.updateDatasetPublicity).toHaveBeenCalledWith(9);
    });

    it("does not open the unpublish modal when the item is already private", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const c = setupComponent({ type: "workflow" });
      c.verifyUnpublish();
      expect(modalServiceSpy.create).not.toHaveBeenCalled();
    });
  });

  describe("publish / unpublish methods", () => {
    it("publishWorkflow flips isPublic and shows a success notification", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const c = setupComponent({ type: "workflow" });
      c.publishWorkflow();
      expect(c.isPublic).toBe(true);
      expect(notificationSpy.success).toHaveBeenCalledWith("Workflow published successfully");
    });

    it("publishWorkflow surfaces HttpErrorResponse via NotificationService.error", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      workflowPersistSpy.updateWorkflowIsPublished.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "publish failed" }, status: 500 }))
      );
      const c = setupComponent({ type: "workflow" });
      c.publishWorkflow();
      expect(notificationSpy.error).toHaveBeenCalledWith("publish failed");
    });

    it("unpublishWorkflow flips isPublic to false and shows a success notification", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      const c = setupComponent({ type: "workflow" });
      c.unpublishWorkflow();
      expect(c.isPublic).toBe(false);
      expect(notificationSpy.success).toHaveBeenCalledWith("Workflow unpublished successfully");
    });

    it("publishDataset flips isPublic and shows a success notification", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: false } }));
      const c = setupComponent({ type: "dataset" });
      c.publishDataset();
      expect(c.isPublic).toBe(true);
      expect(notificationSpy.success).toHaveBeenCalledWith("Dataset published successfully");
    });

    it("publishDataset surfaces HttpErrorResponse via NotificationService.error", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: false } }));
      datasetServiceSpy.updateDatasetPublicity.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "dataset publish failed" }, status: 500 }))
      );
      const c = setupComponent({ type: "dataset" });
      c.publishDataset();
      expect(notificationSpy.error).toHaveBeenCalledWith("dataset publish failed");
    });

    it("unpublishDataset flips isPublic to false and shows a success notification", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: true } }));
      const c = setupComponent({ type: "dataset" });
      c.unpublishDataset();
      expect(c.isPublic).toBe(false);
      expect(notificationSpy.success).toHaveBeenCalledWith("Dataset unpublished successfully");
    });
  });

  describe("hasWriteAccess without a resolved email", () => {
    it("returns false when the current user has no email at all", () => {
      const c = setupComponent();
      // Exercise the no-email early-return guard directly, independent of how the
      // user service happens to resolve an empty/absent email.
      c.currentEmail = undefined;
      expect(c.hasWriteAccess).toBe(false);
    });
  });

  describe("removeEmailTag", () => {
    it("removes the matching email and keeps the others", () => {
      const c = setupComponent();
      c.emailTags = ["a@example.com", "b@example.com", "c@example.com"];
      c.removeEmailTag("b@example.com");
      expect(c.emailTags).toEqual(["a@example.com", "c@example.com"]);
    });

    it("leaves tags unchanged when the email is not present", () => {
      const c = setupComponent();
      c.emailTags = ["a@example.com"];
      c.removeEmailTag("missing@example.com");
      expect(c.emailTags).toEqual(["a@example.com"]);
    });
  });

  describe("onChange", () => {
    it("filters allOwners case-insensitively by the typed value", () => {
      const c = setupComponent();
      c.allOwners.push("Alice", "Bob", "alfred");
      c.onChange("al");
      expect(c.filteredOwners).toEqual(["Alice", "alfred"]);
    });

    it("clears filteredOwners when the value is null", () => {
      const c = setupComponent();
      c.allOwners.push("Alice");
      c.filteredOwners = ["stale"];
      c.onChange(null as unknown as string);
      expect(c.filteredOwners).toEqual([]);
    });
  });

  describe("onPaste with an empty existing value", () => {
    it("defaults the existing email value to an empty string before appending", () => {
      const c = setupComponent();
      c.validateForm.get("email")?.reset();
      const event = {
        preventDefault: vi.fn(),
        clipboardData: { getData: vi.fn().mockReturnValue("solo@example.com") },
      } as unknown as ClipboardEvent;
      c.onPaste(event);
      expect(c.emailTags).toEqual(["solo@example.com"]);
    });
  });

  describe("modal Cancel buttons", () => {
    function captureModalRefs(): any[] {
      const modalRefs: any[] = [];
      modalServiceSpy.create.mockImplementation((config: any) => {
        capturedModalConfigs.push(config);
        const ref = { close: vi.fn() };
        modalRefs.push(ref);
        return ref;
      });
      return modalRefs;
    }

    it("closes the revoke confirmation modal without revoking when Cancel is clicked", () => {
      const modalRefs = captureModalRefs();
      const c = setupComponent({ currentEmail: "me@example.com" });
      c.verifyRevokeAccess("other@example.com");
      getFooterButton(capturedModalConfigs[0], "Cancel").onClick();
      expect(modalRefs[0].close).toHaveBeenCalled();
      expect(accessServiceSpy.revokeAccess).not.toHaveBeenCalled();
    });

    it("closes the downgrade modal and reloads without granting when Cancel is clicked", () => {
      accessServiceSpy.getAccessList.mockReturnValue(
        of([{ email: "me@example.com", name: "Me", privilege: Privilege.WRITE }])
      );
      const modalRefs = captureModalRefs();
      const c = setupComponent({ currentEmail: "me@example.com", type: "workflow", id: 3 });
      accessServiceSpy.grantAccess.mockClear();
      accessServiceSpy.getAccessList.mockClear();
      c.changeAccessLevel("me@example.com", "READ");
      getFooterButton(capturedModalConfigs[0], "Cancel").onClick();
      expect(modalRefs[0].close).toHaveBeenCalled();
      expect(accessServiceSpy.grantAccess).not.toHaveBeenCalled();
      // Cancel re-runs ngOnInit to restore the previous access level in the UI
      expect(accessServiceSpy.getAccessList).toHaveBeenCalledWith("workflow", 3);
    });

    it("closes the publish modal without publishing when Cancel is clicked", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const modalRefs = captureModalRefs();
      const c = setupComponent({ type: "workflow", inWorkspace: true });
      c.verifyPublish();
      getFooterButton(capturedModalConfigs[0], "Cancel").onClick();
      expect(modalRefs[0].close).toHaveBeenCalled();
      expect(workflowPersistSpy.updateWorkflowIsPublished).not.toHaveBeenCalled();
      expect(workflowActionSpy.setWorkflowIsPublished).not.toHaveBeenCalled();
    });

    it("closes the unpublish modal without unpublishing when Cancel is clicked", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      const modalRefs = captureModalRefs();
      const c = setupComponent({ type: "workflow" });
      c.verifyUnpublish();
      getFooterButton(capturedModalConfigs[0], "Cancel").onClick();
      expect(modalRefs[0].close).toHaveBeenCalled();
      expect(workflowPersistSpy.updateWorkflowIsPublished).not.toHaveBeenCalled();
    });
  });

  describe("applyAccessLevelChange error branch", () => {
    it("surfaces HttpErrorResponse and reloads the access list on failure", () => {
      accessServiceSpy.grantAccess.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "change failed" }, status: 500 }))
      );
      const c = setupComponent({ currentEmail: "me@example.com", type: "workflow", id: 3 });
      accessServiceSpy.getAccessList.mockClear();
      c.changeAccessLevel("other@example.com", "READ");
      expect(notificationSpy.error).toHaveBeenCalledWith("change failed");
      // the error branch reloads the access list so the UI reflects the unchanged level
      expect(accessServiceSpy.getAccessList).toHaveBeenCalledWith("workflow", 3);
    });
  });

  describe("unpublish error branches", () => {
    it("unpublishWorkflow surfaces HttpErrorResponse and leaves isPublic unchanged", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.updateWorkflowIsPublished.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "unpublish failed" }, status: 500 }))
      );
      const c = setupComponent({ type: "workflow" });
      c.unpublishWorkflow();
      expect(notificationSpy.error).toHaveBeenCalledWith("unpublish failed");
      expect(c.isPublic).toBe(true);
    });

    it("unpublishDataset surfaces HttpErrorResponse and leaves isPublic unchanged", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: true } }));
      datasetServiceSpy.updateDatasetPublicity.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "dataset unpublish failed" }, status: 500 }))
      );
      const c = setupComponent({ type: "dataset" });
      c.unpublishDataset();
      expect(notificationSpy.error).toHaveBeenCalledWith("dataset unpublish failed");
      expect(c.isPublic).toBe(true);
    });
  });

  describe("guard branches (no-ops)", () => {
    it("handleInputConfirm skips empty tokens produced by trailing separators", () => {
      const c = setupComponent();
      c.validateForm.get("email")?.setValue("a@example.com, ; ");
      c.handleInputConfirm();
      expect(c.emailTags).toEqual(["a@example.com"]);
      expect(messageSpy.error).not.toHaveBeenCalled();
    });

    it("grantAccess does nothing when there are no email tags", () => {
      const c = setupComponent({ type: "workflow", id: 5 });
      accessServiceSpy.grantAccess.mockClear();
      c.emailTags = [];
      c.grantAccess();
      expect(accessServiceSpy.grantAccess).not.toHaveBeenCalled();
      expect(gmailSpy.sendEmail).not.toHaveBeenCalled();
    });

    it("publishWorkflow is a no-op when the workflow is already public", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      const c = setupComponent({ type: "workflow" });
      workflowPersistSpy.updateWorkflowIsPublished.mockClear();
      c.publishWorkflow();
      expect(workflowPersistSpy.updateWorkflowIsPublished).not.toHaveBeenCalled();
    });

    it("unpublishWorkflow is a no-op when the workflow is already private", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      const c = setupComponent({ type: "workflow" });
      workflowPersistSpy.updateWorkflowIsPublished.mockClear();
      c.unpublishWorkflow();
      expect(workflowPersistSpy.updateWorkflowIsPublished).not.toHaveBeenCalled();
    });

    it("publishDataset is a no-op when the dataset is already public", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: true } }));
      const c = setupComponent({ type: "dataset" });
      datasetServiceSpy.updateDatasetPublicity.mockClear();
      c.publishDataset();
      expect(datasetServiceSpy.updateDatasetPublicity).not.toHaveBeenCalled();
    });

    it("unpublishDataset is a no-op when the dataset is already private", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: false } }));
      const c = setupComponent({ type: "dataset" });
      datasetServiceSpy.updateDatasetPublicity.mockClear();
      c.unpublishDataset();
      expect(datasetServiceSpy.updateDatasetPublicity).not.toHaveBeenCalled();
    });
  });
});
