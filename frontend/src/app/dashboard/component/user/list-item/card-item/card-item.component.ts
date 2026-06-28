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

import {
  ChangeDetectorRef,
  Component,
  ElementRef,
  EventEmitter,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
  ViewChild,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgIf } from "@angular/common";
import { FormsModule } from "@angular/forms";
import { RouterLink } from "@angular/router";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { NzCheckboxComponent } from "ng-zorro-antd/checkbox";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzModalRef, NzModalService } from "ng-zorro-antd/modal";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { UserAvatarComponent } from "../../user-avatar/user-avatar.component";
import {
  DEFAULT_WORKFLOW_NAME,
  WorkflowPersistService,
} from "src/app/common/service/workflow-persist/workflow-persist.service";
import { firstValueFrom } from "rxjs";
import { HubWorkflowDetailComponent } from "../../../../../hub/component/workflow/detail/hub-workflow-detail.component";
import { ActionType, HubService } from "../../../../../hub/service/hub.service";
import { DownloadService } from "src/app/dashboard/service/user/download/download.service";
import { formatSize } from "src/app/common/util/size-formatter.util";
import { formatRelativeTime, formatCount } from "src/app/common/util/format.util";
import { DatasetService, DEFAULT_DATASET_NAME } from "../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import {
  HUB_DATASET_RESULT_DETAIL,
  HUB_WORKFLOW_RESULT_DETAIL,
  USER_DATASET,
  USER_PROJECT,
  USER_WORKSPACE,
} from "../../../../../app-routing.constant";
import { isDefined } from "../../../../../common/util/predicate";

@UntilDestroy()
@Component({
  selector: "texera-card-item",
  templateUrl: "./card-item.component.html",
  styleUrls: ["./card-item.component.scss"],
  imports: [
    NzCardComponent,
    RouterLink,
    NgIf,
    FormsModule,
    NzCheckboxComponent,
    UserAvatarComponent,
    NzIconDirective,
    NzButtonComponent,
    NzPopconfirmDirective,
    ɵNzTransitionPatchDirective,
  ],
})
export class CardItemComponent implements OnChanges {
  private owners: number[] = [];
  public originalName: string = "";
  public originalDescription: string | undefined = undefined;
  public disableDelete: boolean = false;
  @Input() currentUid: number | undefined;
  @ViewChild("nameInput") nameInput!: ElementRef;
  @ViewChild("descriptionInput") descriptionInput!: ElementRef;
  editingName = false;
  editingDescription = false;
  likeCount: number = 0;
  viewCount = 0;
  entryLink: string[] = [];
  size: number | undefined = 0;
  public iconType: string = "";
  isLiked: boolean = false;
  @Input() isPrivateSearch = false;
  @Input() editable = false;
  private _entry?: DashboardEntry;
  hovering: boolean = false;
  /** The default top image, used when the user has not uploaded a custom one. */
  static readonly DEFAULT_PREVIEW_IMAGE = "assets/card_background.jpg";
  /** Resolved preview/cover image; stays the placeholder until a dataset cover loads. */
  coverImageSrc: string = CardItemComponent.DEFAULT_PREVIEW_IMAGE;

  @Input()
  get entry(): DashboardEntry {
    if (!this._entry) {
      throw new Error("entry property must be provided.");
    }
    return this._entry;
  }

  set entry(value: DashboardEntry) {
    this._entry = value;
  }

  @Output() checkboxChanged = new EventEmitter<void>();
  @Output() deleted = new EventEmitter<void>();
  @Output() duplicated = new EventEmitter<void>();
  @Output() refresh = new EventEmitter<void>();

  constructor(
    private modalService: NzModalService,
    private workflowPersistService: WorkflowPersistService,
    private datasetService: DatasetService,
    private modal: NzModalService,
    private hubService: HubService,
    private downloadService: DownloadService,
    private cdr: ChangeDetectorRef,
    private notificationService: NotificationService
  ) {}

  initializeEntry() {
    if (this.entry.type === "workflow") {
      if (typeof this.entry.id === "number") {
        this.disableDelete = !this.entry.workflow.isOwner;
        this.owners = this.entry.accessibleUserIds;
        if (this.currentUid !== undefined && this.owners.includes(this.currentUid)) {
          this.entryLink = [USER_WORKSPACE, String(this.entry.id)];
        } else {
          this.entryLink = [HUB_WORKFLOW_RESULT_DETAIL, String(this.entry.id)];
        }
        this.size = this.entry.size;
      }
      this.iconType = "project";
    } else if (this.entry.type === "project") {
      this.entryLink = [USER_PROJECT, String(this.entry.id)];
      this.iconType = "container";
    } else if (this.entry.type === "dataset") {
      if (typeof this.entry.id === "number") {
        this.disableDelete = !this.entry.dataset.isOwner;
        this.owners = this.entry.accessibleUserIds;
        if (this.currentUid !== undefined && this.owners.includes(this.currentUid)) {
          this.entryLink = [USER_DATASET, String(this.entry.id)];
        } else {
          this.entryLink = [HUB_DATASET_RESULT_DETAIL, String(this.entry.id)];
        }
        this.iconType = "database";
        this.size = this.entry.size;
        this.loadDatasetCover(this.entry.id);
      }
    } else if (this.entry.type === "file") {
      // not sure where to redirect
      this.iconType = "folder-open";
    } else {
      throw new Error("Unexpected type in DashboardEntry.");
    }
    this.likeCount = this.entry.likeCount;
    this.viewCount = this.entry.viewCount;
    this.isLiked = this.entry.isLiked;
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["entry"]) {
      this.initializeEntry();
    }
  }

  /** Loads the dataset cover into the preview slot, falling back to the placeholder. */
  private loadDatasetCover(did: number): void {
    if (!this.entry.coverImageUrl) {
      return;
    }
    this.datasetService
      .getDatasetCoverUrl(did)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: ({ url }) => {
          this.coverImageSrc = url ?? CardItemComponent.DEFAULT_PREVIEW_IMAGE;
          this.cdr.markForCheck();
        },
        error: () => {
          this.coverImageSrc = CardItemComponent.DEFAULT_PREVIEW_IMAGE;
          this.cdr.markForCheck();
        },
      });
  }

  /** Falls the preview back to the placeholder if the cover image fails to load. */
  onCoverError(): void {
    this.coverImageSrc = CardItemComponent.DEFAULT_PREVIEW_IMAGE;
  }

  onCheckboxChange(entry: DashboardEntry): void {
    entry.checked = !entry.checked;
    this.cdr.markForCheck();
    this.checkboxChanged.emit();
  }

  public async onClickOpenShareAccess(): Promise<void> {
    let modal: NzModalRef<ShareAccessComponent> | undefined;

    if (this.entry.type === "workflow") {
      modal = this.modalService.create({
        nzContent: ShareAccessComponent,
        nzData: {
          writeAccess: this.entry.workflow.accessLevel === "WRITE",
          type: this.entry.type,
          id: this.entry.id,
          allOwners: await firstValueFrom(this.workflowPersistService.retrieveOwners()),
          inWorkspace: false,
        },
        nzFooter: null,
        nzTitle: "Share this workflow with others",
        nzCentered: true,
        nzWidth: "700px",
      });
    } else if (this.entry.type === "dataset") {
      modal = this.modalService.create({
        nzContent: ShareAccessComponent,
        nzData: {
          writeAccess: this.entry.accessLevel === "WRITE",
          type: "dataset",
          id: this.entry.id,
          allOwners: await firstValueFrom(this.datasetService.retrieveOwners()),
        },
        nzFooter: null,
        nzTitle: "Share this dataset with others",
        nzCentered: true,
        nzWidth: "700px",
      });
    }
    if (modal) {
      modal.componentInstance?.refresh.pipe(untilDestroyed(this)).subscribe(() => {
        this.refresh.emit();
      });
    }
  }

  public onClickDownload = (): void => {
    if (!this.entry.id) return;

    if (this.entry.type === "workflow") {
      this.downloadService
        .downloadWorkflow(this.entry.id, this.entry.workflow.workflow.name)
        .pipe(untilDestroyed(this))
        .subscribe();
    } else if (this.entry.type === "dataset") {
      this.downloadService.downloadDataset(this.entry.id, this.entry.name).pipe(untilDestroyed(this)).subscribe();
    }
  };

  onEditName(): void {
    this.originalName = this.entry.name;
    this.editingName = true;
    setTimeout(() => {
      if (this.nameInput) {
        const inputElement = this.nameInput.nativeElement;
        const valueLength = inputElement.value.length;
        inputElement.focus();
        inputElement.setSelectionRange(valueLength, valueLength);
      }
    }, 0);
  }

  onEditDescription(): void {
    this.originalDescription = this.entry.description;
    this.editingDescription = true;
    setTimeout(() => {
      if (this.descriptionInput) {
        const textareaElement = this.descriptionInput.nativeElement;
        const valueLength = textareaElement.value.length;
        textareaElement.focus();
        textareaElement.setSelectionRange(valueLength, valueLength);
      }
    }, 0);
  }

  private updateProperty(
    updateMethod: (id: number, value: string) => any,
    propertyName: "name" | "description",
    newValue: string,
    originalValue: string | undefined
  ): void {
    if (!this.entry.id) {
      this.notificationService.error("Id is missing");
      return;
    }

    updateMethod(this.entry.id, newValue)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.entry[propertyName] = newValue; // Dynamic property assignment
        },
        error: () => {
          this.notificationService.error("Update failed");
          (this.entry as any)[propertyName] = originalValue ?? ""; // Fallback to original value
          this.setEditingState(propertyName, false);
        },
        complete: () => {
          this.setEditingState(propertyName, false);
        },
      });
  }

  private setEditingState(propertyName: "name" | "description", state: boolean): void {
    if (propertyName === "name") {
      this.editingName = state;
    } else if (propertyName === "description") {
      this.editingDescription = state;
    }
  }

  public confirmUpdateCustomName(name: string): void {
    if (this.entry.name === this.originalName) {
      this.editingName = false;
      return;
    }
    const newName = this.entry.type === "workflow" ? name || DEFAULT_WORKFLOW_NAME : name || DEFAULT_DATASET_NAME;

    if (this.entry.type === "workflow") {
      this.updateProperty(
        this.workflowPersistService.updateWorkflowName.bind(this.workflowPersistService),
        "name",
        newName,
        this.originalName
      );
    } else if (this.entry.type === "dataset") {
      this.updateProperty(
        this.datasetService.updateDatasetName.bind(this.datasetService),
        "name",
        newName,
        this.originalName
      );
    }
  }

  public confirmUpdateCustomDescription(description: string | undefined): void {
    if (this.entry.description === this.originalDescription) {
      this.editingDescription = false;
      return;
    }
    const updatedDescription = description ?? "";

    if (this.entry.type === "workflow") {
      this.updateProperty(
        this.workflowPersistService.updateWorkflowDescription.bind(this.workflowPersistService),
        "description",
        updatedDescription,
        this.originalDescription
      );
    } else if (this.entry.type === "dataset") {
      this.updateProperty(
        this.datasetService.updateDatasetDescription.bind(this.datasetService),
        "description",
        updatedDescription,
        this.originalDescription
      );
    }
  }

  openDetailModal(wid: number | undefined): void {
    const modalRef = this.modal.create({
      nzTitle: "Workflow Detail",
      nzContent: HubWorkflowDetailComponent,
      nzData: {
        wid: wid ?? 0,
      },
      nzFooter: null,
      nzStyle: { width: "60%" },
      nzBodyStyle: { maxHeight: "70vh", overflow: "auto" },
    });

    const instance = modalRef.componentInstance;
    if (instance) {
      if (wid !== undefined) {
        this.hubService
          .getCounts([this.entry.type], [wid], [ActionType.View])
          .pipe(untilDestroyed(this))
          .subscribe(counts => {
            const count = counts[0];
            this.viewCount = (count?.counts.view ?? 0) + 1; // hacky fix to display view correctly
          });
      }
    }
  }

  toggleLike(): void {
    const userId = this.currentUid;
    if (!isDefined(userId) || !isDefined(this.entry.id)) {
      return;
    }

    const entryId = this.entry.id!;

    if (this.isLiked) {
      this.hubService
        .postUnlike(entryId, this.entry.type)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = false;
            this.hubService
              .getCounts([this.entry.type], [entryId], [ActionType.Like])
              .pipe(untilDestroyed(this))
              .subscribe(counts => {
                this.likeCount = counts[0].counts.like ?? 0;
              });
          }
        });
    } else {
      this.hubService
        .postLike(entryId, this.entry.type)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = true;
            this.hubService
              .getCounts([this.entry.type], [entryId], [ActionType.Like])
              .pipe(untilDestroyed(this))
              .subscribe(counts => {
                this.likeCount = counts[0].counts.like ?? 0;
              });
          }
        });
    }
  }

  // aliases for the shared formatting utilities so the template can call them directly
  formatTime = formatRelativeTime;
  formatCount = formatCount;
  formatSize = formatSize;
}
