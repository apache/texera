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

import { Component, EventEmitter, inject, OnDestroy, OnInit, Output } from "@angular/core";
import { FormBuilder, FormControl, FormGroup, Validators, FormsModule, ReactiveFormsModule } from "@angular/forms";
import { ShareAccessService } from "../../../service/user/share-access/share-access.service";
import { Privilege, ShareAccess } from "../../../type/share-access.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { UserService } from "../../../../common/service/user/user.service";
import { GmailService } from "../../../../common/service/gmail/gmail.service";
import { NZ_MODAL_DATA, NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { HttpErrorResponse } from "@angular/common/http";
import { forkJoin } from "rxjs";
import { USER_DATASET, USER_PROJECT, USER_WORKFLOW } from "../../../../app-routing.constant";
import { NzMessageService } from "ng-zorro-antd/message";
import { DatasetService } from "../../../service/user/dataset/dataset.service";
import {
  WorkflowPersistService,
  WorkflowPublishStatus,
} from "src/app/common/service/workflow-persist/workflow-persist.service";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { NgIf, NgFor, NgSwitch, NgSwitchCase, NgSwitchDefault, DatePipe } from "@angular/common";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzRowDirective, NzColDirective } from "ng-zorro-antd/grid";
import { NzFormItemComponent, NzFormLabelComponent, NzFormControlComponent } from "ng-zorro-antd/form";
import { NzInputDirective } from "ng-zorro-antd/input";
import { NzAutocompleteTriggerDirective, NzAutocompleteComponent } from "ng-zorro-antd/auto-complete";
import { NzTagComponent } from "ng-zorro-antd/tag";
import { NzTooltipDirective } from "ng-zorro-antd/tooltip";
import { NzSegmentedComponent } from "ng-zorro-antd/segmented";
import { NzBadgeComponent } from "ng-zorro-antd/badge";

@UntilDestroy()
@Component({
  selector: "texera-share-access",
  templateUrl: "share-access.component.html",
  styleUrls: ["./share-access.component.scss"],
  imports: [
    NgIf,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    NzWaveDirective,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    FormsModule,
    ReactiveFormsModule,
    NzCardComponent,
    NzRowDirective,
    NzFormItemComponent,
    NzColDirective,
    NzFormLabelComponent,
    NzFormControlComponent,
    NzInputDirective,
    NzAutocompleteTriggerDirective,
    NzAutocompleteComponent,
    NgFor,
    NgSwitch,
    NgSwitchCase,
    NgSwitchDefault,
    DatePipe,
    NzTagComponent,
    NzTooltipDirective,
    NzSegmentedComponent,
    NzBadgeComponent,
  ],
})
export class ShareAccessComponent implements OnInit, OnDestroy {
  readonly nzModalData = inject(NZ_MODAL_DATA);
  readonly type: string = this.nzModalData.type;
  readonly id: number = this.nzModalData.id;
  readonly allOwners: string[] = this.nzModalData.allOwners;
  readonly inWorkspace: boolean = this.nzModalData.inWorkspace;
  public validateForm: FormGroup;
  public accessList: ReadonlyArray<ShareAccess> = [];
  public owner: string = "";
  public filteredOwners: Array<string> = [];
  public ownerSearchValue?: string;
  public emailTags: string[] = [];
  currentEmail: string | undefined = "";
  isPublic: boolean | null = null;
  /** Undefined until fetched, and for anyone who cannot publish (the endpoint needs write access). */
  publishStatus?: WorkflowPublishStatus;
  isPinning = false;
  /** The two sides of the switch; the control only takes strings or numbers, so 1 means pinned. */
  readonly publicCopyOptions = [
    { label: "Follow latest", value: 0 },
    { label: "Pinned", value: 1 },
  ];
  /** To the second, always: the revision panel names the same version that way. */
  readonly publicationTimeFormat = "MMM d, HH:mm:ss";
  private shouldRefresh = false;
  @Output() refresh = new EventEmitter<void>();

  constructor(
    private accessService: ShareAccessService,
    private formBuilder: FormBuilder,
    private userService: UserService,
    private gmailService: GmailService,
    private notificationService: NotificationService,
    private message: NzMessageService,
    private modalService: NzModalService,
    private workflowPersistService: WorkflowPersistService,
    private datasetService: DatasetService,
    private workflowActionService: WorkflowActionService,
    private modalRef: NzModalRef
  ) {
    this.validateForm = this.formBuilder.group({
      email: [null, Validators.email],
      accessLevel: ["WRITE"],
    });
    this.currentEmail = this.userService.getCurrentUser()?.email;
  }

  get hasWriteAccess(): boolean {
    if (!this.currentEmail) {
      return false;
    }
    if (this.currentEmail === this.owner) {
      return true;
    }
    const currentUserAccess = this.accessList.find(entry => entry.email === this.currentEmail);
    return currentUserAccess?.privilege === Privilege.WRITE;
  }

  ngOnInit(): void {
    const accessList$ = this.accessService.getAccessList(this.type, this.id);
    const owner$ = this.accessService.getOwner(this.type, this.id);

    if (this.type === "workflow") {
      // Joined rather than subscribed separately because the publish state depends on all three:
      // hasWriteAccess is only answerable once the owner and the access list have landed, and the
      // strip only applies to a published workflow.
      forkJoin([accessList$, owner$, this.workflowPersistService.getWorkflowIsPublished(this.id)])
        .pipe(untilDestroyed(this))
        .subscribe(([accessList, owner, workflowType]) => {
          this.accessList = accessList;
          this.owner = owner;
          this.isPublic = workflowType === "Public";
          // Asked of the saved copy, so make sure it is the copy the author is looking at. The
          // editor saves a few seconds after the last edit, and this dialog is modal -- so the only
          // way to reach it with unsaved edits is to open it inside that window, which is exactly
          // when an author comes to check what the public can see. Flushing here answers about the
          // canvas in front of them instead of about the canvas as of a few seconds ago.
          this.flushPendingEdits();
          this.refreshPublishStatus();
        });
      // The panel describes the saved copy and the editor saves on a debounce, so re-read when a
      // save lands -- until then the server would still answer with the copy before it.
      this.workflowPersistService
        .getWorkflowPersistedStream()
        .pipe(untilDestroyed(this))
        .subscribe(() => this.refreshPublishStatus());
      return;
    }

    accessList$.pipe(untilDestroyed(this)).subscribe(access => (this.accessList = access));
    owner$.pipe(untilDestroyed(this)).subscribe(name => {
      this.owner = name;
    });
    if (this.type === "dataset") {
      this.datasetService
        .getDataset(this.id)
        .pipe(untilDestroyed(this))
        .subscribe(dashboardDataset => {
          this.isPublic = dashboardDataset.dataset.isPublic;
        });
    }
  }

  ngOnDestroy(): void {
    if (this.shouldRefresh) {
      this.refresh.emit();
    }
  }

  /**
   * Saves whatever the editor is holding, so the panel describes the canvas in front of the author.
   * Only inside the workspace, and only while saving is on: previewing an old version turns
   * persistence off, and writing the preview back would be a save nobody asked for.
   */
  private flushPendingEdits(): void {
    if (!this.inWorkspace || !this.isPublic || !this.hasWriteAccess) {
      return;
    }
    if (!this.workflowPersistService.isWorkflowPersistEnabled()) {
      return;
    }
    this.workflowPersistService
      .persistWorkflow(this.workflowActionService.getWorkflow())
      .pipe(untilDestroyed(this))
      .subscribe({ error: () => {} });
  }

  /** Only for a published workflow the user can publish -- the endpoint requires write access. */
  private refreshPublishStatus(): void {
    if (this.type !== "workflow" || !this.isPublic || !this.hasWriteAccess) {
      this.publishStatus = undefined;
      return;
    }
    this.workflowPersistService
      .getPublishStatus(this.id)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: status => (this.publishStatus = status),
        error: () => (this.publishStatus = undefined),
      });
  }

  /** Named as a state, so the switch, the sentence and the dot cannot disagree about which is in force. */
  get publishState(): "follow" | "pinned" | "behind" {
    if (!this.publishStatus?.isPinned) {
      return "follow";
    }
    return this.publishStatus.hasUnpublishedChanges ? "behind" : "pinned";
  }

  /**
   * Changes what the public sees, leaving the author's working copy alone. Picking the side already
   * in force does nothing, so a stray click cannot silently publish edits made since the pin;
   * `republish` is how the card asks for exactly that.
   */
  public choosePublicCopy(pinned: boolean, republish = false): void {
    if (!this.publishStatus || (pinned === this.publishStatus.isPinned && !republish)) {
      return;
    }
    this.isPinning = true;
    const change = pinned
      ? { request: this.workflowPersistService.pinLatestVersion(this.id), done: "The public now sees this version" }
      : { request: this.workflowPersistService.unpinVersion(this.id), done: "The public now follows your latest" };
    change.request
      .pipe(untilDestroyed(this))
      .subscribe({
        next: status => {
          this.publishStatus = status;
          this.notificationService.success(change.done);
        },
        error: (error: unknown) => {
          if (error instanceof HttpErrorResponse) {
            this.notificationService.error(error.error.message);
          }
        },
      })
      .add(() => (this.isPinning = false));
  }

  public handleInputConfirm(event?: Event): void {
    if (event) {
      event.preventDefault();
    }
    const emailInput = this.validateForm.get("email")?.value;

    if (emailInput) {
      const emailArray: string[] = emailInput.split(/[\s,;]+/);
      emailArray.forEach(email => {
        if (email) {
          const emailControl = new FormControl(email, Validators.email);
          if (!emailControl.errors && !this.emailTags.includes(email)) {
            this.emailTags.push(email);
          } else if (this.emailTags.includes(email)) {
            this.message.error(`${email} is already in the tags`);
          } else {
            this.message.error(`${email} is not a valid email`);
          }
        }
      });
    }

    this.validateForm.get("email")?.reset();
  }

  public removeEmailTag(email: string): void {
    this.emailTags = this.emailTags.filter(tag => tag !== email);
  }

  public grantAccess(): void {
    this.handleInputConfirm();
    if (this.emailTags.length > 0) {
      this.emailTags.forEach(email => {
        let message = `${this.userService.getCurrentUser()?.email} shared a ${this.type} with you`;
        if (this.type !== "computing-unit") {
          let routePath = "";
          if (this.type === "workflow") routePath = USER_WORKFLOW;
          if (this.type === "dataset") routePath = USER_DATASET;
          if (this.type === "project") routePath = USER_PROJECT;
          message += `, access the ${this.type} at ${location.origin}${routePath}/${this.id}`;
        }
        this.accessService
          .grantAccess(this.type, this.id, email, this.validateForm.value.accessLevel)
          .pipe(untilDestroyed(this))
          .subscribe({
            next: () => {
              this.notificationService.success(this.type + " shared with " + email + " successfully.");
              this.gmailService.sendEmail(
                "Texera: " + this.userService.getCurrentUser()?.email + " shared a " + this.type + " with you",
                message,
                email
              );
              this.ngOnInit();
            },
            error: (error: unknown) => {
              if (error instanceof HttpErrorResponse) {
                this.notificationService.error(error.error.message);
              }
            },
          });
      });
      this.emailTags = [];
    }
  }

  public onPaste(event: ClipboardEvent): void {
    event.preventDefault();
    const pasteData = event.clipboardData?.getData("text");
    if (pasteData) {
      const currentEmailValue = this.validateForm.get("email")?.value || "";
      // concaste new emails and old emails
      const newValue = currentEmailValue + pasteData;
      this.validateForm.get("email")?.setValue(newValue);
      this.handleInputConfirm();
    }
  }

  public onChange(value: string): void {
    if (value === null || value === undefined) {
      this.filteredOwners = [];
    } else {
      this.filteredOwners = this.allOwners.filter(owner => owner.toLowerCase().indexOf(value.toLowerCase()) !== -1);
    }
  }

  public verifyRevokeAccess(userToRemove: string): void {
    const isRevokingOwnAccess = userToRemove === this.userService.getCurrentUser()?.email;
    const modalTitle = isRevokingOwnAccess ? "Revoke Your Access" : "Revoke Access";
    const modalContent = isRevokingOwnAccess
      ? `Are you sure you want to revoke your own access to this ${this.type}? You will no longer be able to view or edit it.`
      : `Are you sure you want to revoke ${userToRemove}'s access to this ${this.type}?`;

    const modal: NzModalRef = this.modalService.create({
      nzTitle: modalTitle,
      nzContent: modalContent,
      nzFooter: [
        {
          label: "Cancel",
          onClick: () => modal.close(),
        },
        {
          label: "Revoke",
          type: "primary",
          danger: true,
          onClick: () => {
            this.revokeAccess(userToRemove);
            modal.close();
          },
        },
      ],
    });
  }

  private revokeAccess(userToRemove: string): void {
    this.accessService
      .revokeAccess(this.type, this.id, userToRemove)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          if (userToRemove == this.userService.getCurrentUser()?.email) {
            this.shouldRefresh = true;
            this.modalRef.close({ userRevokedOwnAccess: true });
          }
          this.ngOnInit();
        },
        error: (error: unknown) => {
          if (error instanceof HttpErrorResponse) {
            this.notificationService.error(error.error.message);
          }
        },
      });
  }

  public changeAccessLevel(email: string, newPrivilege: string): void {
    const isOwnAccess = email === this.currentEmail;
    const currentUserAccess = this.accessList.find(entry => entry.email === email);
    const isDowngrade = currentUserAccess?.privilege === Privilege.WRITE && newPrivilege === "READ";

    if (isOwnAccess && isDowngrade) {
      const modal: NzModalRef = this.modalService.create({
        nzTitle: "Downgrade Your Access",
        nzContent: `Are you sure you want to change your own access to READ? You will no longer be able to edit this ${this.type} or manage access.`,
        nzFooter: [
          {
            label: "Cancel",
            onClick: () => {
              modal.close();
              this.ngOnInit();
            },
          },
          {
            label: "Confirm",
            type: "primary",
            danger: true,
            onClick: () => {
              this.applyAccessLevelChange(email, newPrivilege);
              modal.close();
            },
          },
        ],
      });
    } else {
      this.applyAccessLevelChange(email, newPrivilege);
    }
  }

  private applyAccessLevelChange(email: string, newPrivilege: string): void {
    this.accessService
      .grantAccess(this.type, this.id, email, newPrivilege)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.notificationService.success(`Access level for ${email} changed to ${newPrivilege}.`);
          this.ngOnInit();
        },
        error: (error: unknown) => {
          if (error instanceof HttpErrorResponse) {
            this.notificationService.error(error.error.message);
          }
          this.ngOnInit();
        },
      });
  }

  public verifyPublish(): void {
    if (!this.isPublic) {
      const modal: NzModalRef = this.modalService.create({
        nzTitle: "Notice",
        nzContent:
          `Publishing your ${this.type} would grant all Texera users read access to your ${this.type} along with the right to clone your work.` +
          (this.type === "workflow"
            ? " The public will follow your latest version as you save it, unless you pin one."
            : ""),
        nzFooter: [
          {
            label: "Cancel",
            onClick: () => modal.close(),
          },
          {
            label: "Publish",
            type: "primary",
            onClick: () => {
              if (this.type === "workflow") {
                this.publishWorkflow();

                if (this.inWorkspace) {
                  this.workflowActionService.setWorkflowIsPublished(1);
                }
              } else if (this.type === "dataset") {
                this.publishDataset();
              }
              modal.close();
            },
          },
        ],
      });
    }
  }

  public verifyUnpublish(): void {
    if (this.isPublic) {
      const modal: NzModalRef = this.modalService.create({
        nzTitle: "Notice",
        nzContent: `All other users would lose access to your ${this.type} if you unpublish it.`,
        nzFooter: [
          {
            label: "Cancel",
            onClick: () => modal.close(),
          },
          {
            label: "Unpublish",
            type: "primary",
            onClick: () => {
              if (this.type === "workflow") {
                this.unpublishWorkflow();
                if (this.inWorkspace) {
                  this.workflowActionService.setWorkflowIsPublished(0);
                }
              } else if (this.type === "dataset") {
                this.unpublishDataset();
              }
              modal.close();
            },
          },
        ],
      });
    }
  }

  public publishWorkflow(): void {
    if (!this.isPublic) {
      this.workflowPersistService
        .updateWorkflowIsPublished(this.id, true)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.isPublic = true;
            this.refreshPublishStatus();
            this.notificationService.success("Workflow published successfully");
          },
          error: (error: unknown) => {
            if (error instanceof HttpErrorResponse) {
              this.notificationService.error(error.error.message);
            }
          },
        });
    }
  }

  public unpublishWorkflow(): void {
    if (this.isPublic) {
      this.workflowPersistService
        .updateWorkflowIsPublished(this.id, false)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.isPublic = false;
            this.publishStatus = undefined;
            this.notificationService.success("Workflow unpublished successfully");
          },
          error: (error: unknown) => {
            if (error instanceof HttpErrorResponse) {
              this.notificationService.error(error.error.message);
            }
          },
        });
    }
  }

  public publishDataset(): void {
    if (!this.isPublic) {
      this.datasetService
        .updateDatasetPublicity(this.id)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.isPublic = true;
            this.notificationService.success("Dataset published successfully");
          },
          error: (error: unknown) => {
            if (error instanceof HttpErrorResponse) {
              this.notificationService.error(error.error.message);
            }
          },
        });
    }
  }

  public unpublishDataset(): void {
    if (this.isPublic) {
      this.datasetService
        .updateDatasetPublicity(this.id)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.isPublic = false;
            this.notificationService.success("Dataset unpublished successfully");
          },
          error: (error: unknown) => {
            if (error instanceof HttpErrorResponse) {
              this.notificationService.error(error.error.message);
            }
          },
        });
    }
  }
}
