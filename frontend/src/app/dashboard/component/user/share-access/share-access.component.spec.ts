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
import { By } from "@angular/platform-browser";
import { HttpErrorResponse } from "@angular/common/http";
import { of, throwError } from "rxjs";

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
  };
  let datasetServiceSpy: {
    getDataset: ReturnType<typeof vi.fn>;
    updateDatasetPublicity: ReturnType<typeof vi.fn>;
  };
  let workflowActionSpy: { setWorkflowIsPublished: ReturnType<typeof vi.fn> };
  let userServiceCurrentEmail: string | undefined;
  // The component reads publicity back after writing it, so the doubles have to hold state:
  // the workflow endpoint sets it absolutely, the dataset one toggles.
  let workflowPublished: boolean;
  let datasetPublished: boolean;
  let capturedModalConfigs: any[];
  /** The NzModalRef stubs handed back by modalService.create, in creation order. */
  let capturedModalRefs: { close: ReturnType<typeof vi.fn> }[];
  /**
   * The fixture built by the most recent setupComponent() call, for the template-level tests.
   * Cleared in beforeEach: a test that reads it without having built one then fails on the spot
   * rather than silently querying the previous test's detached DOM.
   */
  let fixture: ComponentFixture<ShareAccessComponent>;

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
    fixture = TestBed.createComponent(ShareAccessComponent);
    fixture.detectChanges();
    return fixture.componentInstance;
  }

  beforeEach(() => {
    TestBed.resetTestingModule();
    fixture = undefined as unknown as ComponentFixture<ShareAccessComponent>;
    capturedModalConfigs = [];
    capturedModalRefs = [];
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
        const ref = { close: vi.fn() };
        capturedModalRefs.push(ref);
        return ref;
      }),
    };
    workflowPublished = false;
    datasetPublished = false;
    workflowPersistSpy = {
      getWorkflowIsPublished: vi.fn(() => of(workflowPublished ? "Public" : "Private")),
      updateWorkflowIsPublished: vi.fn((_id: number, next: boolean) => {
        workflowPublished = next;
        return of(null);
      }),
    };
    datasetServiceSpy = {
      getDataset: vi.fn(() => of({ dataset: { isPublic: datasetPublished } })),
      updateDatasetPublicity: vi.fn(() => {
        datasetPublished = !datasetPublished;
        return of(null);
      }),
    };
    workflowActionSpy = { setWorkflowIsPublished: vi.fn() };
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
      workflowPublished = true;
      const c = setupComponent({ type: "workflow", id: 9 });
      expect(workflowPersistSpy.getWorkflowIsPublished).toHaveBeenCalledWith(9);
      expect(c.isPublic).toBe(true);
    });

    it("sets isPublic to false when workflow publish state is Private", () => {
      workflowPublished = false;
      const c = setupComponent({ type: "workflow" });
      expect(c.isPublic).toBe(false);
    });

    it("loads publish state for dataset via DatasetService.getDataset", () => {
      datasetPublished = true;
      const c = setupComponent({ type: "dataset", id: 12 });
      expect(datasetServiceSpy.getDataset).toHaveBeenCalledWith(12);
      expect(c.isPublic).toBe(true);
    });

    it("does not query publish state for non-workflow/dataset types", () => {
      setupComponent({ type: "file", id: 4 });
      expect(workflowPersistSpy.getWorkflowIsPublished).not.toHaveBeenCalled();
      expect(datasetServiceSpy.getDataset).not.toHaveBeenCalled();
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
      workflowPublished = false;
      const c = setupComponent({ type: "workflow", id: 8, inWorkspace: true });
      c.verifyPublish();
      getFooterButton(capturedModalConfigs[0], "Publish").onClick();
      expect(workflowPersistSpy.updateWorkflowIsPublished).toHaveBeenCalledWith(8, true);
      expect(workflowActionSpy.setWorkflowIsPublished).toHaveBeenCalledWith(1);
    });

    it("does not call WorkflowActionService.setWorkflowIsPublished when not inWorkspace", () => {
      workflowPublished = false;
      const c = setupComponent({ type: "workflow", id: 8, inWorkspace: false });
      c.verifyPublish();
      getFooterButton(capturedModalConfigs[0], "Publish").onClick();
      expect(workflowActionSpy.setWorkflowIsPublished).not.toHaveBeenCalled();
    });

    it("publishes a dataset on confirm", () => {
      datasetPublished = false;
      const c = setupComponent({ type: "dataset", id: 9 });
      c.verifyPublish();
      getFooterButton(capturedModalConfigs[0], "Publish").onClick();
      expect(datasetServiceSpy.updateDatasetPublicity).toHaveBeenCalledWith(9);
    });

    it("warns about cloning only for a clonable kind", () => {
      workflowPublished = false;
      setupComponent({ type: "workflow" }).verifyPublish();
      expect(capturedModalConfigs[0].nzContent).toContain("the right to clone your work");

      TestBed.resetTestingModule();
      capturedModalConfigs = [];
      datasetPublished = false;
      setupComponent({ type: "dataset" }).verifyPublish();
      expect(capturedModalConfigs[0].nzContent).toContain("read access");
      expect(capturedModalConfigs[0].nzContent).not.toContain("clone");
    });

    it("does not open the publish modal when the item is already public", () => {
      workflowPublished = true;
      const c = setupComponent({ type: "workflow" });
      c.verifyPublish();
      expect(modalServiceSpy.create).not.toHaveBeenCalled();
    });

    it("unpublishes a workflow on confirm and updates the action service when inWorkspace", () => {
      workflowPublished = true;
      const c = setupComponent({ type: "workflow", id: 8, inWorkspace: true });
      c.verifyUnpublish();
      getFooterButton(capturedModalConfigs[0], "Unpublish").onClick();
      expect(workflowPersistSpy.updateWorkflowIsPublished).toHaveBeenCalledWith(8, false);
      expect(workflowActionSpy.setWorkflowIsPublished).toHaveBeenCalledWith(0);
    });

    it("unpublishes a dataset on confirm", () => {
      datasetPublished = true;
      const c = setupComponent({ type: "dataset", id: 9 });
      c.verifyUnpublish();
      getFooterButton(capturedModalConfigs[0], "Unpublish").onClick();
      expect(datasetServiceSpy.updateDatasetPublicity).toHaveBeenCalledWith(9);
    });

    it("does not open the unpublish modal when the item is already private", () => {
      workflowPublished = false;
      const c = setupComponent({ type: "workflow" });
      c.verifyUnpublish();
      expect(modalServiceSpy.create).not.toHaveBeenCalled();
    });
  });

  describe("setPublished", () => {
    it("publishing a workflow flips isPublic and shows a success notification", () => {
      workflowPublished = false;
      const c = setupComponent({ type: "workflow" });
      c.setPublished(true);
      expect(c.isPublic).toBe(true);
      expect(notificationSpy.success).toHaveBeenCalledWith("Workflow published successfully");
    });

    it("a failed workflow publish surfaces HttpErrorResponse via NotificationService.error", () => {
      workflowPublished = false;
      workflowPersistSpy.updateWorkflowIsPublished.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "publish failed" }, status: 500 }))
      );
      const c = setupComponent({ type: "workflow" });
      c.setPublished(true);
      expect(notificationSpy.error).toHaveBeenCalledWith("publish failed");
    });

    it("unpublishing a workflow flips isPublic to false and shows a success notification", () => {
      workflowPublished = true;
      const c = setupComponent({ type: "workflow" });
      c.setPublished(false);
      expect(c.isPublic).toBe(false);
      expect(notificationSpy.success).toHaveBeenCalledWith("Workflow unpublished successfully");
    });

    it("publishing a dataset flips isPublic and shows a success notification", () => {
      datasetPublished = false;
      const c = setupComponent({ type: "dataset" });
      c.setPublished(true);
      expect(c.isPublic).toBe(true);
      expect(notificationSpy.success).toHaveBeenCalledWith("Dataset published successfully");
    });

    it("a failed dataset publish surfaces HttpErrorResponse via NotificationService.error", () => {
      datasetPublished = false;
      datasetServiceSpy.updateDatasetPublicity.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "dataset publish failed" }, status: 500 }))
      );
      const c = setupComponent({ type: "dataset" });
      c.setPublished(true);
      expect(notificationSpy.error).toHaveBeenCalledWith("dataset publish failed");
    });

    it("unpublishing a dataset flips isPublic to false and shows a success notification", () => {
      datasetPublished = true;
      const c = setupComponent({ type: "dataset" });
      c.setPublished(false);
      expect(c.isPublic).toBe(false);
      expect(notificationSpy.success).toHaveBeenCalledWith("Dataset unpublished successfully");
    });

    it("reports what the server has, not what was asked for, when the toggle disagrees", () => {
      datasetPublished = false;
      const c = setupComponent({ type: "dataset", id: 9 });
      datasetPublished = true;

      c.setPublished(true);

      expect(c.isPublic).toBe(false);
      expect(notificationSpy.success).toHaveBeenCalledWith("Dataset unpublished successfully");
    });

    it("keeps a landed publish when the read-back fails", () => {
      // The write succeeded; only the confirming read broke. Calling that a failure would invite a
      // retry, and the retry would toggle the resource straight back.
      datasetPublished = false;
      const c = setupComponent({ type: "dataset", id: 9 });
      datasetServiceSpy.getDataset.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "lakefs down" }, status: 500 }))
      );

      c.setPublished(true);

      expect(datasetServiceSpy.updateDatasetPublicity).toHaveBeenCalledWith(9);
      expect(c.isPublic).toBe(true);
      expect(notificationSpy.success).toHaveBeenCalledWith("Dataset published successfully");
      expect(notificationSpy.error).not.toHaveBeenCalled();
    });

    it("does nothing for a registered kind that cannot be published", () => {
      const c = setupComponent({ type: "file", id: 4 });
      c.setPublished(true);
      expect(c.isPublic).toBeNull();
      expect(notificationSpy.success).not.toHaveBeenCalled();
    });

    it("does nothing for a kind the registry does not carry at all", () => {
      const c = setupComponent({ type: "computing-unit", id: 4 });
      c.setPublished(true);
      expect(c.isPublic).toBeNull();
      expect(notificationSpy.success).not.toHaveBeenCalled();
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
      workflowPublished = false;
      const modalRefs = captureModalRefs();
      const c = setupComponent({ type: "workflow", inWorkspace: true });
      c.verifyPublish();
      getFooterButton(capturedModalConfigs[0], "Cancel").onClick();
      expect(modalRefs[0].close).toHaveBeenCalled();
      expect(workflowPersistSpy.updateWorkflowIsPublished).not.toHaveBeenCalled();
      expect(workflowActionSpy.setWorkflowIsPublished).not.toHaveBeenCalled();
    });

    it("closes the unpublish modal without unpublishing when Cancel is clicked", () => {
      workflowPublished = true;
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
    it("a failed workflow unpublish surfaces HttpErrorResponse and leaves isPublic unchanged", () => {
      workflowPublished = true;
      workflowPersistSpy.updateWorkflowIsPublished.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "unpublish failed" }, status: 500 }))
      );
      const c = setupComponent({ type: "workflow" });
      c.setPublished(false);
      expect(notificationSpy.error).toHaveBeenCalledWith("unpublish failed");
      expect(c.isPublic).toBe(true);
    });

    it("a failed dataset unpublish surfaces HttpErrorResponse and leaves isPublic unchanged", () => {
      datasetPublished = true;
      datasetServiceSpy.updateDatasetPublicity.mockReturnValue(
        throwError(() => new HttpErrorResponse({ error: { message: "dataset unpublish failed" }, status: 500 }))
      );
      const c = setupComponent({ type: "dataset" });
      c.setPublished(false);
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

    it("publishing is a no-op when the workflow is already public", () => {
      workflowPublished = true;
      const c = setupComponent({ type: "workflow" });
      workflowPersistSpy.updateWorkflowIsPublished.mockClear();
      c.setPublished(true);
      expect(workflowPersistSpy.updateWorkflowIsPublished).not.toHaveBeenCalled();
    });

    it("unpublishing is a no-op when the workflow is already private", () => {
      workflowPublished = false;
      const c = setupComponent({ type: "workflow" });
      workflowPersistSpy.updateWorkflowIsPublished.mockClear();
      c.setPublished(false);
      expect(workflowPersistSpy.updateWorkflowIsPublished).not.toHaveBeenCalled();
    });

    it("publishing is a no-op when the dataset is already public", () => {
      datasetPublished = true;
      const c = setupComponent({ type: "dataset" });
      datasetServiceSpy.updateDatasetPublicity.mockClear();
      c.setPublished(true);
      expect(datasetServiceSpy.updateDatasetPublicity).not.toHaveBeenCalled();
    });

    it("unpublishing is a no-op when the dataset is already private", () => {
      datasetPublished = false;
      const c = setupComponent({ type: "dataset" });
      datasetServiceSpy.updateDatasetPublicity.mockClear();
      c.setPublished(false);
      expect(datasetServiceSpy.updateDatasetPublicity).not.toHaveBeenCalled();
    });
  });

  /**
   * Everything above drives the component's methods directly. The template decides which control
   * reaches which of those methods, and with what argument — the publish pair and the per-row
   * access controls are near-symmetric, so a crossed binding would look right on screen and do the
   * opposite thing.
   */
  describe("template wiring", () => {
    /** Makes the current user the owner, which is what enables the write-gated controls. */
    function asOwner(): void {
      accessServiceSpy.getOwner.mockReturnValue(of("me@example.com"));
    }

    /**
     * The publish pair, addressed by the label the user reads rather than by DOM position. That is
     * the mapping under test: a crossed (click) handler still fails, while reordering the two
     * buttons no longer fails *this* test. Document order is a separate contract and is pinned by
     * its own test below, so addressing by label gives up nothing. The uniqueness check is what
     * keeps a dropped or duplicated button from reading as a pass.
     */
    function accessButton(label: "Private" | "Public"): HTMLButtonElement {
      const matches = fixture.debugElement
        .queryAll(By.css("button.access-button"))
        .filter(
          button =>
            (button.nativeElement as HTMLElement).querySelector(".button-text-header")?.textContent?.trim() === label
        );
      expect(matches).toHaveLength(1);
      return matches[0].nativeElement as HTMLButtonElement;
    }

    it("offers the restrictive option first: Private, then Public", () => {
      asOwner();
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      setupComponent({ type: "workflow" });

      // accessButton() is deliberately blind to order so the two mapping tests below fail only
      // for a crossed binding. Order is still a contract of its own — this pair is how the user
      // is asked to think about visibility, and the narrower choice is presented first — so it is
      // pinned here explicitly rather than riding along as a side effect of a positional
      // destructure, where a reorder and a crossed handler were indistinguishable.
      const labels = fixture.debugElement
        .queryAll(By.css("button.access-button .button-text-header"))
        .map(header => (header.nativeElement as HTMLElement).textContent?.trim());
      expect(labels).toEqual(["Private", "Public"]);
    });

    it("puts the unpublish confirmation behind Private and the publish confirmation behind Public", () => {
      asOwner();
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      setupComponent({ type: "workflow" });

      // Already private, so Private has nothing to confirm.
      accessButton("Private").click();
      expect(modalServiceSpy.create).not.toHaveBeenCalled();

      accessButton("Public").click();
      expect(capturedModalConfigs).toHaveLength(1);
      expect(capturedModalConfigs[0].nzContent).toContain("Publishing your workflow");
    });

    it("keeps that pairing when the workflow is already public", () => {
      asOwner();
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      setupComponent({ type: "workflow" });

      // Already public, so Public has nothing to confirm.
      accessButton("Public").click();
      expect(modalServiceSpy.create).not.toHaveBeenCalled();

      accessButton("Private").click();
      expect(capturedModalConfigs).toHaveLength(1);
      expect(capturedModalConfigs[0].nzContent).toContain("lose access to your workflow");
    });

    it("renders one closable tag per queued email and drops only the tag that was closed", () => {
      const c = setupComponent();
      c.emailTags = ["first@example.com", "second@example.com"];
      fixture.detectChanges();

      // Scoped to the form: the access list below it carries an OWNER tag of its own.
      const tags = fixture.debugElement.queryAll(By.css("form nz-tag"));
      expect(tags.map(tag => (tag.nativeElement as HTMLElement).textContent?.trim())).toEqual([
        "first@example.com",
        "second@example.com",
      ]);

      // nzMode drives whether nz-tag emits a close control at all; firing nzOnClose by hand would
      // pass just as well against a tag the user can never dismiss.
      tags.forEach(tag => expect((tag.nativeElement as HTMLElement).querySelector(".ant-tag-close-icon")).toBeTruthy());

      // Closing the second tag must not take the first one with it.
      tags[1].triggerEventHandler("nzOnClose", new MouseEvent("click"));

      expect(c.emailTags).toEqual(["first@example.com"]);
    });

    it("applies a level change and a revoke to the row they were issued from", () => {
      asOwner();
      accessServiceSpy.getAccessList.mockReturnValue(
        of([{ email: "other@example.com", name: "Other", privilege: Privilege.READ }])
      );
      setupComponent({ type: "workflow", id: 3 });
      accessServiceSpy.grantAccess.mockClear();

      const select = fixture.debugElement.query(By.css("ul.current-share select")).nativeElement as HTMLSelectElement;
      expect(select.value).toBe("READ");
      // dispatchEvent fires listeners on a disabled control too, so the gate has to be read off
      // the element rather than inferred from the call going through.
      expect(select.disabled).toBe(false);
      select.value = "WRITE";
      select.dispatchEvent(new Event("change"));

      // The email and the new privilege must not swap places.
      expect(accessServiceSpy.grantAccess).toHaveBeenCalledWith("workflow", 3, "other@example.com", "WRITE");

      const revoke = fixture.debugElement.query(By.css("ul.current-share li button"))
        .nativeElement as HTMLButtonElement;
      revoke.click();

      expect(capturedModalConfigs).toHaveLength(1);
      expect(capturedModalConfigs[0].nzContent).toContain("revoke other@example.com's access");
    });

    it("leaves every write-gated control live for the owner", () => {
      asOwner();
      setupComponent({ type: "workflow" });

      const buttons = fixture.debugElement.queryAll(By.css("button.access-button"));
      expect(buttons.map(b => (b.nativeElement as HTMLButtonElement).disabled)).toEqual([false, false]);
      const submit = fixture.debugElement.query(By.css('form button[type="submit"]'))
        .nativeElement as HTMLButtonElement;
      expect(submit.disabled).toBe(false);
    });

    it("locks the write-gated controls for a read-only viewer, except the one that drops their own access", () => {
      accessServiceSpy.getOwner.mockReturnValue(of("owner@example.com"));
      accessServiceSpy.getAccessList.mockReturnValue(
        of([
          { email: "me@example.com", name: "Me", privilege: Privilege.READ },
          { email: "other@example.com", name: "Other", privilege: Privilege.READ },
        ])
      );
      const c = setupComponent({ type: "workflow", currentEmail: "me@example.com" });
      expect(c.hasWriteAccess).toBe(false);

      const buttons = fixture.debugElement.queryAll(By.css("button.access-button"));
      expect(buttons.map(b => (b.nativeElement as HTMLButtonElement).disabled)).toEqual([true, true]);
      const submit = fixture.debugElement.query(By.css('form button[type="submit"]'))
        .nativeElement as HTMLButtonElement;
      expect(submit.disabled).toBe(true);

      const selects = fixture.debugElement.queryAll(By.css("ul.current-share li select"));
      expect(selects.map(s => (s.nativeElement as HTMLSelectElement).disabled)).toEqual([true, true]);

      // Leaving a resource you can only read is still yours to do, so your own row keeps its
      // revoke button live while everybody else's is locked.
      const revokes = fixture.debugElement.queryAll(By.css("ul.current-share li button"));
      expect(revokes.map(b => (b.nativeElement as HTMLButtonElement).disabled)).toEqual([false, true]);
    });
  });

  /**
   * Only HttpErrorResponse carries the `error.message` these handlers read, so every subscription
   * narrows before touching it. A transport-level failure therefore reaches the user as nothing at
   * all — pinned here so the guard cannot be dropped, which would turn each of these into a
   * TypeError thrown out of the error callback.
   */
  describe("non-HTTP failures", () => {
    const offline = () => throwError(() => new Error("offline"));

    it("sharing: no notification either way", () => {
      accessServiceSpy.grantAccess.mockReturnValue(offline());
      const c = setupComponent({ type: "workflow", id: 5 });
      c.emailTags = ["a@example.com"];

      c.grantAccess();

      expect(notificationSpy.error).not.toHaveBeenCalled();
      expect(notificationSpy.success).not.toHaveBeenCalled();
    });

    it("revoking: no notification, and the modal still closes", () => {
      accessServiceSpy.revokeAccess.mockReturnValue(offline());
      const c = setupComponent({ currentEmail: "me@example.com" });

      c.verifyRevokeAccess("other@example.com");
      getFooterButton(capturedModalConfigs[0], "Revoke").onClick();

      expect(accessServiceSpy.revokeAccess).toHaveBeenCalled();
      expect(notificationSpy.error).not.toHaveBeenCalled();
      // The confirmation is dismissed by the button itself, not by the response, so a failed
      // revoke must not leave the dialog open over the list.
      expect(capturedModalRefs[0].close).toHaveBeenCalled();
    });

    it("changing a level: no notification, but the list is still reloaded", () => {
      accessServiceSpy.grantAccess.mockReturnValue(offline());
      const c = setupComponent({ currentEmail: "me@example.com", type: "workflow", id: 3 });
      accessServiceSpy.getAccessList.mockClear();

      c.changeAccessLevel("other@example.com", "READ");

      expect(notificationSpy.error).not.toHaveBeenCalled();
      expect(accessServiceSpy.getAccessList).toHaveBeenCalledWith("workflow", 3);
    });

    it("publishing a workflow: no notification, and it stays private", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Private"));
      workflowPersistSpy.updateWorkflowIsPublished.mockReturnValue(offline());
      const c = setupComponent({ type: "workflow" });

      c.setPublished(true);

      expect(workflowPersistSpy.updateWorkflowIsPublished).toHaveBeenCalledWith(1, true);
      expect(notificationSpy.error).not.toHaveBeenCalled();
      expect(c.isPublic).toBe(false);
    });

    it("unpublishing a workflow: no notification, and it stays public", () => {
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      workflowPersistSpy.updateWorkflowIsPublished.mockReturnValue(offline());
      const c = setupComponent({ type: "workflow" });

      c.setPublished(false);

      expect(workflowPersistSpy.updateWorkflowIsPublished).toHaveBeenCalledWith(1, false);
      expect(notificationSpy.error).not.toHaveBeenCalled();
      expect(c.isPublic).toBe(true);
    });

    it("publishing a dataset: no notification, and it stays private", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: false } }));
      datasetServiceSpy.updateDatasetPublicity.mockReturnValue(offline());
      const c = setupComponent({ type: "dataset" });

      c.setPublished(true);

      expect(datasetServiceSpy.updateDatasetPublicity).toHaveBeenCalledWith(1);
      expect(notificationSpy.error).not.toHaveBeenCalled();
      expect(c.isPublic).toBe(false);
    });

    it("unpublishing a dataset: no notification, and it stays public", () => {
      datasetServiceSpy.getDataset.mockReturnValue(of({ dataset: { isPublic: true } }));
      datasetServiceSpy.updateDatasetPublicity.mockReturnValue(offline());
      const c = setupComponent({ type: "dataset" });

      c.setPublished(false);

      expect(datasetServiceSpy.updateDatasetPublicity).toHaveBeenCalledWith(1);
      expect(notificationSpy.error).not.toHaveBeenCalled();
      expect(c.isPublic).toBe(true);
    });
  });

  describe("confirmation for kinds that carry no publicity", () => {
    it("unpublishing a workflow outside the workspace leaves the canvas state alone", () => {
      // setWorkflowIsPublished only exists to keep an open editor in step; there is no editor here.
      workflowPersistSpy.getWorkflowIsPublished.mockReturnValue(of("Public"));
      const c = setupComponent({ type: "workflow", id: 8, inWorkspace: false });

      c.verifyUnpublish();
      getFooterButton(capturedModalConfigs[0], "Unpublish").onClick();

      expect(workflowPersistSpy.updateWorkflowIsPublished).toHaveBeenCalledWith(8, false);
      expect(workflowActionSpy.setWorkflowIsPublished).not.toHaveBeenCalled();
    });

    it("confirming Publish on a kind with no publish endpoint does nothing", () => {
      const c = setupComponent({ type: "computing-unit", id: 4 });

      c.verifyPublish();
      getFooterButton(capturedModalConfigs[0], "Publish").onClick();

      expect(workflowPersistSpy.updateWorkflowIsPublished).not.toHaveBeenCalled();
      expect(datasetServiceSpy.updateDatasetPublicity).not.toHaveBeenCalled();
    });

    it("confirming Unpublish on a kind with no publish endpoint does nothing", () => {
      const c = setupComponent({ type: "computing-unit", id: 4 });
      c.isPublic = true;

      c.verifyUnpublish();
      getFooterButton(capturedModalConfigs[0], "Unpublish").onClick();

      expect(workflowPersistSpy.updateWorkflowIsPublished).not.toHaveBeenCalled();
      expect(datasetServiceSpy.updateDatasetPublicity).not.toHaveBeenCalled();
    });
  });
});
