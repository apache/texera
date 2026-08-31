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
import { of, switchMap } from "rxjs";
import { NzMessageService } from "ng-zorro-antd/message";
import { WorkflowActionService } from "src/app/workspace/service/workflow-graph/model/workflow-action.service";
import { ResourceRegistryService } from "../../../service/user/resource-registry/resource-registry.service";
import { ResourceDescriptor } from "../../../type/resource-descriptor";
import { EntityType } from "../../../../hub/service/hub.service";
import { NgIf, NgFor } from "@angular/common";
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
    NzTagComponent,
    NzTooltipDirective,
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
  /** Undefined for kinds the registry does not carry, i.e. computing units. */
  private readonly descriptor: ResourceDescriptor | undefined;
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
    private workflowActionService: WorkflowActionService,
    private resourceRegistry: ResourceRegistryService,
    private modalRef: NzModalRef
  ) {
    this.validateForm = this.formBuilder.group({
      email: [null, Validators.email],
      accessLevel: ["WRITE"],
    });
    this.currentEmail = this.userService.getCurrentUser()?.email;
    this.descriptor = this.resourceRegistry.find(this.type as EntityType);
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
    this.accessService
      .getAccessList(this.type, this.id)
      .pipe(untilDestroyed(this))
      .subscribe(access => (this.accessList = access));
    this.accessService
      .getOwner(this.type, this.id)
      .pipe(untilDestroyed(this))
      .subscribe(name => {
        this.owner = name;
      });
    // Stays null for kinds that cannot be published, which is what hides the publish buttons.
    this.descriptor
      ?.isPublic?.(this.id)
      .pipe(untilDestroyed(this))
      .subscribe(isPublic => (this.isPublic = isPublic));
  }

  ngOnDestroy(): void {
    if (this.shouldRefresh) {
      this.refresh.emit();
    }
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
        const routePath = this.descriptor?.privateRoute;
        if (routePath !== undefined) {
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
      // Only a clonable kind hands out more than read access, so only it carries the warning.
      const cloneWarning = this.descriptor?.affordances?.clonable ? ", along with the right to clone your work" : "";
      const modal: NzModalRef = this.modalService.create({
        nzTitle: "Notice",
        nzContent: `Publishing your ${this.type} would grant all Texera users read access to your ${this.type}${cloneWarning}.`,
        nzFooter: [
          {
            label: "Cancel",
            onClick: () => modal.close(),
          },
          {
            label: "Publish",
            type: "primary",
            onClick: () => {
              this.setPublished(true);
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
              this.setPublished(false);
              modal.close();
            },
          },
        ],
      });
    }
  }

  public setPublished(next: boolean): void {
    const descriptor = this.descriptor;
    if (!descriptor?.setPublished || this.isPublic === next) {
      return;
    }
    const readBack = descriptor.isPublic;
    const label = this.type.charAt(0).toUpperCase() + this.type.slice(1);
    descriptor
      .setPublished(this.id, next)
      // Toggle-style backends ignore `next`, so the server's own answer decides what is reported.
      .pipe(
        switchMap(() => (readBack ? readBack(this.id) : of(next))),
        untilDestroyed(this)
      )
      .subscribe({
        next: published => {
          this.isPublic = published;
          if (this.inWorkspace) {
            this.workflowActionService.setWorkflowIsPublished(published ? 1 : 0);
          }
          this.notificationService.success(`${label} ${published ? "published" : "unpublished"} successfully`);
        },
        error: (error: unknown) => {
          if (error instanceof HttpErrorResponse) {
            this.notificationService.error(error.error.message);
          }
        },
      });
  }
}
