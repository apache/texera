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
import { HttpClientTestingModule } from "@angular/common/http/testing";
import { NoopAnimationsModule } from "@angular/platform-browser/animations";
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
  let capturedModalConfigs: any[];

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
    return fixture.componentInstance;
  }

  beforeEach(() => {
    TestBed.resetTestingModule();
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
    workflowPersistSpy = {
      getWorkflowIsPublished: vi.fn().mockReturnValue(of("Private")),
      updateWorkflowIsPublished: vi.fn().mockReturnValue(of(null)),
    };
    datasetServiceSpy = {
      getDataset: vi.fn().mockReturnValue(of({ dataset: { isPublic: false } })),
      updateDatasetPublicity: vi.fn().mockReturnValue(of(null)),
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
});
