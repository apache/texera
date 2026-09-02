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
import { NzModalService } from "ng-zorro-antd/modal";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { UserAvatarComponent } from "../../user-avatar/user-avatar.component";
import { firstValueFrom, Observable } from "rxjs";
import { HubWorkflowDetailComponent } from "../../../../../hub/component/workflow/detail/hub-workflow-detail.component";
import { ActionType, HubService } from "../../../../../hub/service/hub.service";
import { formatSize } from "src/app/common/util/size-formatter.util";
import { formatRelativeTime, formatCount } from "src/app/common/util/format.util";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { extractErrorMessage } from "../../../../../common/util/error";
import { WorkflowCoverService } from "../../../../service/user/workflow-cover/workflow-cover.service";
import { isDefined } from "../../../../../common/util/predicate";
import { ResourceRegistryService } from "../../../../service/user/resource-registry/resource-registry.service";

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
  public originalName: string = "";
  public originalDescription: string | undefined = undefined;
  public disableDelete: boolean = false;
  public canDownload: boolean = false;
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
  /** Whether this kind can be shared at all; the button is hidden when it cannot. */
  public canShare: boolean = false;
  /** Resolved preview/cover image; stays the placeholder until a file-backed cover loads. */
  coverImageSrc: string = CardItemComponent.DEFAULT_PREVIEW_IMAGE;

  /** The workflow's custom cover image data URL, if one has been set. */
  private customImage?: string;
  @ViewChild("backgroundInput") backgroundInput!: ElementRef<HTMLInputElement>;

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
    private modal: NzModalService,
    private hubService: HubService,
    private cdr: ChangeDetectorRef,
    private notificationService: NotificationService,
    private workflowCoverService: WorkflowCoverService,
    private resourceRegistry: ResourceRegistryService
  ) {}

  get hasCustomImage(): boolean {
    return this.customImage !== undefined;
  }

  /** Whether the cover-image controls are shown: an editable workflow in private search. */
  get canEditCover(): boolean {
    return this.isPrivateSearch && this.entry.type === "workflow" && this.entry.workflow.isOwner;
  }

  openImagePicker(): void {
    this.backgroundInput?.nativeElement.click();
  }

  async onImageSelected(event: Event): Promise<void> {
    const input = event.target as HTMLInputElement;
    const file = input.files?.[0];
    input.value = ""; // allow re-selecting the same file
    if (!file) {
      return;
    }
    if (!file.type.startsWith("image/")) {
      this.notificationService.error("Please choose an image file.");
      return;
    }
    if (typeof this.entry.id !== "number") {
      return;
    }
    try {
      this.customImage = await this.workflowCoverService.setCoverFromFile(this.entry.id, file);
      this.coverImageSrc = this.customImage;
      this.cdr.markForCheck();
    } catch (e) {
      this.notificationService.error("Failed to set the cover image.");
    }
  }

  resetImage(): void {
    if (typeof this.entry.id !== "number") {
      return;
    }
    this.workflowCoverService
      .clearCover(this.entry.id)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.customImage = undefined;
          this.coverImageSrc = CardItemComponent.DEFAULT_PREVIEW_IMAGE;
          this.cdr.markForCheck();
        },
        error: () => this.notificationService.error("Failed to reset the cover image."),
      });
  }

  initializeEntry() {
    this.coverImageSrc = CardItemComponent.DEFAULT_PREVIEW_IMAGE;
    this.customImage = undefined;
    const descriptor = this.resourceRegistry.get(this.entry.type);
    this.iconType = descriptor.iconType;
    this.disableDelete = !descriptor.isOwner(this.entry);
    this.canDownload = descriptor.download !== undefined;
    this.canShare = descriptor.retrieveOwners !== undefined;
    this.entryLink = this.resourceRegistry.entryLink(this.entry, this.currentUid);
    if (descriptor.hasSize && typeof this.entry.id === "number") {
      this.size = this.entry.size;
    }
    // A workflow cover is a data URL already on the entry; the file-backed kinds have to fetch one.
    if (this.entry.type === "workflow") {
      this.coverImageSrc = this.entry.coverImageUrl ?? CardItemComponent.DEFAULT_PREVIEW_IMAGE;
      this.customImage = this.entry.coverImageUrl ?? undefined;
    } else if (descriptor.coverUrl && this.entry.coverImageUrl && typeof this.entry.id === "number") {
      this.loadCover(descriptor.coverUrl, this.entry.id);
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

  /** Loads a file-backed cover into the preview slot, falling back to the placeholder. */
  private loadCover(coverUrl: (id: number) => Observable<string | null>, id: number): void {
    coverUrl(id)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: url => {
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
    const retrieveOwners = this.resourceRegistry.get(this.entry.type).retrieveOwners;
    if (!retrieveOwners) {
      return;
    }
    const modal = this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: this.entry.accessLevel === "WRITE",
        type: this.entry.type,
        id: this.entry.id,
        allOwners: await firstValueFrom(retrieveOwners()),
        inWorkspace: false,
      },
      nzFooter: null,
      nzTitle: `Share this ${this.entry.type} with others`,
      nzCentered: true,
      nzWidth: "700px",
    });
    modal.componentInstance?.refresh.pipe(untilDestroyed(this)).subscribe(() => {
      this.refresh.emit();
    });
  }

  public onClickDownload = (): void => {
    const download = this.resourceRegistry.get(this.entry.type).download;
    if (!this.entry.id || !download) return;
    download(this.entry.id, this.entry.name).pipe(untilDestroyed(this)).subscribe();
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
        error: (err: unknown) => {
          this.notificationService.error(extractErrorMessage(err));
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
    const descriptor = this.resourceRegistry.get(this.entry.type);
    if (!descriptor.rename) {
      return;
    }
    const newName = name || descriptor.defaultName || "";

    const nameError = descriptor.validateName?.(newName);
    if (nameError) {
      this.notificationService.error(nameError);
      this.entry.name = this.originalName;
      this.editingName = false;
      return;
    }

    this.updateProperty(descriptor.rename, "name", newName, this.originalName);
  }

  public confirmUpdateCustomDescription(description: string | undefined): void {
    if (this.entry.description === this.originalDescription) {
      this.editingDescription = false;
      return;
    }
    const descriptor = this.resourceRegistry.get(this.entry.type);
    if (!descriptor.updateDescription) {
      return;
    }
    this.updateProperty(descriptor.updateDescription, "description", description ?? "", this.originalDescription);
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
