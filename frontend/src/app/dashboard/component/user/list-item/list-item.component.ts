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
  ElementRef,
  EventEmitter,
  Input,
  OnChanges,
  Output,
  SimpleChanges,
  ViewChild,
} from "@angular/core";
import { Component } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { MarkdownDescriptionComponent } from "../markdown-description/markdown-description.component";
import { ShareAccessComponent } from "../share-access/share-access.component";
import { firstValueFrom } from "rxjs";
import { HubWorkflowDetailComponent } from "../../../../hub/component/workflow/detail/hub-workflow-detail.component";
import { ActionType, HubService } from "../../../../hub/service/hub.service";
import { formatSize } from "src/app/common/util/size-formatter.util";
import { formatCount, formatRelativeTime } from "src/app/common/util/format.util";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { extractErrorMessage } from "../../../../common/util/error";
import { isDefined } from "../../../../common/util/predicate";
import { ResourceRegistryService } from "../../../service/user/resource-registry/resource-registry.service";
import { NzCardComponent } from "ng-zorro-antd/card";
import { NzRowDirective, NzColDirective } from "ng-zorro-antd/grid";
import { RouterLink } from "@angular/router";
import { NgIf, NgClass } from "@angular/common";
import { ɵNzTransitionPatchDirective } from "ng-zorro-antd/core/transition-patch";
import { NzIconDirective } from "ng-zorro-antd/icon";
import { NzSpaceCompactItemDirective } from "ng-zorro-antd/space";
import { NzButtonComponent } from "ng-zorro-antd/button";
import { FormsModule } from "@angular/forms";
import { UserAvatarComponent } from "../user-avatar/user-avatar.component";
import { NzWaveDirective } from "ng-zorro-antd/core/wave";
import { NzPopconfirmDirective } from "ng-zorro-antd/popconfirm";
import { GuiConfigService } from "../../../../common/service/gui-config.service";
import { DefaultView } from "../../../type/workflow-metadata.interface";
import { defaultsToFormView, landingLink } from "./default-view-landing";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";

@UntilDestroy()
@Component({
  selector: "texera-list-item",
  templateUrl: "./list-item.component.html",
  styleUrls: ["./list-item.component.scss"],
  imports: [
    NzCardComponent,
    NzRowDirective,
    RouterLink,
    NzColDirective,
    NgIf,
    NgClass,
    ɵNzTransitionPatchDirective,
    NzIconDirective,
    NzSpaceCompactItemDirective,
    NzButtonComponent,
    FormsModule,
    UserAvatarComponent,
    NzWaveDirective,
    NzPopconfirmDirective,
  ],
})
export class ListItemComponent implements OnChanges {
  public originalName: string = "";
  public originalDescription: string | undefined = undefined;
  public disableDelete: boolean = false;
  public canDownload: boolean = false;
  /** Whether this kind can be shared at all; the button is hidden when it cannot. */
  public canShare: boolean = false;
  @Input() currentUid: number | undefined;
  @ViewChild("nameInput") nameInput!: ElementRef;
  @ViewChild("descriptionInput") descriptionInput!: ElementRef;
  editingName = false;
  editingDescription = false;
  renderedDescription = "";

  likeCount: number = 0;
  viewCount = 0;
  entryLink: string[] = [];
  size: number | undefined = 0;
  public iconType: string = "";
  /** Whether this workflow opens in the Form View by default. */
  public defaultsToForm = false;
  isLiked: boolean = false;
  @Input() isPrivateSearch = false;

  /**
   * Whether the default-view toggle is offered, and honoured: setting the default view writes the
   * workflow row (the endpoint requires WRITE), so a read-only collaborator gets no control that
   * could only fail, and the handler itself checks the same rule rather than trusting the template.
   */
  get canToggleDefaultView(): boolean {
    return this.entry.type === "workflow" && this.config.env.formViewEnabled && this.entry.accessLevel === "WRITE";
  }
  @Input() editable = false;
  private _entry?: DashboardEntry;
  hovering: boolean = false;

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
    private resourceRegistry: ResourceRegistryService,
    protected config: GuiConfigService,
    private workflowPersistService: WorkflowPersistService
  ) {}

  initializeEntry() {
    const descriptor = this.resourceRegistry.get(this.entry.type);
    this.iconType = descriptor.iconType;
    this.disableDelete = !descriptor.isOwner(this.entry);
    this.canDownload = descriptor.download !== undefined;
    this.canShare = descriptor.retrieveOwners !== undefined;
    this.entryLink = this.resourceRegistry.entryLink(this.entry, this.currentUid);
    if (descriptor.hasSize && typeof this.entry.id === "number") {
      this.size = this.entry.size;
    }
    // A workflow opens in its default view: with the feature flag on, a form-default one shows
    // the Form View icon and deep-links straight into the form; a canvas-default one is unchanged.
    this.applyDefaultView();
    this.likeCount = this.entry.likeCount;
    this.viewCount = this.entry.viewCount;
    this.isLiked = this.entry.isLiked;
  }

  private renderMarkdownPreview(text: string | undefined): void {
    const trimmed = (text ?? "").trim();
    if (!trimmed) {
      this.renderedDescription = "";
      return;
    }
    this.renderedDescription = trimmed
      .replace(/[#*_~`>|]/g, "")
      .replace(/\[([^\]]*)\]\([^)]*\)/g, "$1") // [text](url) → text
      .replace(/\s+/g, " ")
      .trim();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["entry"]) {
      this.initializeEntry();
      this.renderMarkdownPreview(this.entry.description);
    }
  }

  /**
   * A workflow opens in its default view. A form-default one is marked with the Form View icon
   * and its owner's row deep-links straight into the form; the operator canvas is still one
   * click away from there. A canvas-default one is left as the descriptor set it. Hub links are
   * untouched; only the owner's own entry point moves. The rule itself lives in
   * default-view-landing.ts, shared with the card renderer so both views of the dashboard agree.
   */
  private applyDefaultView(): void {
    const formViewEnabled = this.config.env.formViewEnabled;
    this.defaultsToForm = defaultsToFormView(this.entry, formViewEnabled);
    if (this.defaultsToForm) {
      this.iconType = "solution";
    }
    this.entryLink = landingLink(this.entry, this.entryLink, formViewEnabled);
  }

  public onToggleDefaultView(): void {
    if (!this.canToggleDefaultView) {
      return;
    }
    const next = this.defaultsToForm ? DefaultView.CANVAS : DefaultView.FORM;
    this.workflowPersistService
      .setDefaultView(this.entry.id as number, next)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          if (this.entry.workflow?.workflow) {
            this.entry.workflow.workflow.defaultView = next;
          }
          // Re-derive from the descriptor so the icon and link fully reset when turning it
          // off, not just when turning it on.
          this.initializeEntry();
          this.cdr.detectChanges();
        },
        error: (err: unknown) => this.notificationService.error(extractErrorMessage(err)),
      });
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
    if (!this.editable) return;

    this.originalDescription = this.entry.description;

    const modalRef = this.modalService.create<MarkdownDescriptionComponent>({
      nzTitle: "Edit Description",
      nzContent: MarkdownDescriptionComponent,
      nzData: {
        description: this.entry.description ?? "",
      },
      nzFooter: null,
      nzWidth: "800px",
    });

    modalRef.componentInstance?.descriptionChange.pipe(untilDestroyed(this)).subscribe(desc => {
      this.confirmUpdateCustomDescription(desc);
      modalRef.destroy();
    });
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
          if (propertyName === "description") {
            this.renderMarkdownPreview(newValue);
          }
        },
        error: (err: unknown) => {
          this.notificationService.error(extractErrorMessage(err));
          (this.entry as any)[propertyName] = originalValue ?? ""; // Fallback to original value
          if (propertyName === "description") {
            this.renderMarkdownPreview(originalValue);
          }
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
    const descriptor = this.resourceRegistry.get(this.entry.type);
    if (!descriptor.updateDescription) {
      return;
    }
    this.updateProperty(descriptor.updateDescription, "description", description ?? "", this.originalDescription);
  }

  formatRelativeTime = formatRelativeTime;

  openDetailModal(wid: number | undefined): void {
    const modalRef = this.modal.create({
      nzTitle: "Workflow Detail",
      nzContent: HubWorkflowDetailComponent,
      nzData: {
        wid: wid ?? 0,
      },
      nzFooter: null,
      nzWidth: "max(900px, 60vw)",
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

  formatCount = formatCount;

  // alias for formatSize
  formatSize = formatSize;
}
