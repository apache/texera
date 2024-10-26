import {
  Component,
  EventEmitter,
  Input,
  Output,
  OnInit,
  OnChanges,
  SimpleChanges,
  ViewChild,
  ElementRef,
  ChangeDetectorRef,
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { ShareAccessComponent } from "../share-access/share-access.component";
import {
  WorkflowPersistService,
  DEFAULT_WORKFLOW_NAME,
} from "src/app/common/service/workflow-persist/workflow-persist.service";
import { firstValueFrom } from "rxjs";
import { SearchService } from "../../../service/user/search.service";
import { HubWorkflowDetailComponent } from "../../../../hub/component/workflow/detail/hub-workflow-detail.component";
import { HubWorkflowService } from "../../../../hub/service/workflow/hub-workflow.service";
import { DownloadService } from "src/app/dashboard/service/user/download/download.service";
import { formatSize } from "src/app/common/util/size-formatter.util";

@UntilDestroy()
@Component({
  selector: "texera-list-item",
  templateUrl: "./list-item.component.html",
  styleUrls: ["./list-item.component.scss"],
})
export class ListItemComponent implements OnInit, OnChanges {
  private owners: number[] = [];
  public originalName: string = "";
  public originalDescription: string | undefined = undefined;
  @Input() currentUid: number | undefined;
  @ViewChild("nameInput") nameInput!: ElementRef;
  @ViewChild("descriptionInput") descriptionInput!: ElementRef;
  editingName = false;
  editingDescription = false;
  likeCount: number = 0;

  ROUTER_WORKFLOW_BASE_URL = "/dashboard/user/workspace";
  ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user/project";
  ROUTER_DATASET_BASE_URL = "/dashboard/user/dataset";
  ROUTER_WORKFLOW_DETAIL_BASE_URL = "/dashboard/hub/workflow/search/result/detail";
  entryLink: string[] = [];
  public iconType: string = "";
  isLiked: boolean = false;
  @Input() isPrivateSearch = false;
  @Input() editable = false;
  private _entry?: DashboardEntry;
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

  @Output() deleted = new EventEmitter<void>();
  @Output() duplicated = new EventEmitter<void>();
  @Output()
  refresh = new EventEmitter<void>();

  constructor(
    private searchService: SearchService,
    private modalService: NzModalService,
    private workflowPersistService: WorkflowPersistService,
    private modal: NzModalService,
    private hubWorkflowService: HubWorkflowService,
    private downloadService: DownloadService,
    private cdr: ChangeDetectorRef
  ) {}

  initializeEntry() {
    if (this.entry.type === "workflow") {
      if (typeof this.entry.id === "number") {
        // eslint-disable-next-line rxjs-angular/prefer-takeuntil
        this.searchService.getWorkflowOwners(this.entry.id).subscribe((data: number[]) => {
          this.owners = data;
          if (this.currentUid !== undefined && this.owners.includes(this.currentUid)) {
            this.entryLink = [this.ROUTER_WORKFLOW_BASE_URL, String(this.entry.id)];
          } else {
            this.entryLink = [this.ROUTER_WORKFLOW_DETAIL_BASE_URL, String(this.entry.id)];
          }
          setTimeout(() => this.cdr.detectChanges(), 0);
        });
        this.hubWorkflowService
          .getLikeCount(this.entry.id)
          .pipe(untilDestroyed(this))
          .subscribe(count => {
            this.likeCount = count;
          });
      }
      // this.entryLink = this.ROUTER_WORKFLOW_BASE_URL + "/" + this.entry.id;
      this.iconType = "project";
    } else if (this.entry.type === "project") {
      this.entryLink = [this.ROUTER_USER_PROJECT_BASE_URL, String(this.entry.id)];
      this.iconType = "container";
    } else if (this.entry.type === "dataset") {
      this.entryLink = [this.ROUTER_DATASET_BASE_URL, String(this.entry.id)];
      this.iconType = "database";
    } else if (this.entry.type === "file") {
      // not sure where to redirect
      this.iconType = "folder-open";
    } else {
      throw new Error("Unexpected type in DashboardEntry.");
    }
  }

  ngOnInit(): void {
    this.initializeEntry();
    if (this.entry.id !== undefined && this.currentUid !== undefined) {
      this.hubWorkflowService
        .isWorkflowLiked(this.entry.id, this.currentUid)
        .pipe(untilDestroyed(this))
        .subscribe((isLiked: boolean) => {
          this.isLiked = isLiked;
        });
    }
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["entry"]) {
      this.initializeEntry();
    }
    if (this.entry.id !== undefined && this.currentUid !== undefined) {
      this.hubWorkflowService
        .isWorkflowLiked(this.entry.id, this.currentUid)
        .pipe(untilDestroyed(this))
        .subscribe((isLiked: boolean) => {
          this.isLiked = isLiked;
        });
    }
  }

  public async onClickOpenShareAccess(): Promise<void> {
    if (this.entry.type === "workflow") {
      this.modalService.create({
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
      this.modalService.create({
        nzContent: ShareAccessComponent,
        nzData: {
          writeAccess: this.entry.accessLevel === "WRITE",
          type: "dataset",
          id: this.entry.id,
        },
        nzFooter: null,
        nzTitle: "Share this dataset with others",
        nzCentered: true,
        nzWidth: "700px",
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

  private updateWorkflowProperty(
    updateMethod: (id: number | undefined, value: string) => any,
    propertyName: "name" | "description",
    newValue: string,
    originalValue: string | undefined
  ): void {
    updateMethod(this.entry.id, newValue)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.entry[propertyName] = newValue;
        },
        error: (err: unknown) => {
          console.error(`Failed to update workflow ${propertyName}:`, err);
          // Use a fallback empty string if originalValue is undefined
          this.entry[propertyName] = originalValue ?? "";
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

  public confirmUpdateWorkflowCustomName(name: string): void {
    const workflowName = name || DEFAULT_WORKFLOW_NAME;
    this.updateWorkflowProperty(
      this.workflowPersistService.updateWorkflowName.bind(this.workflowPersistService),
      "name",
      workflowName,
      this.originalName
    );
  }

  public confirmUpdateWorkflowCustomDescription(description: string | undefined): void {
    const updatedDescription = description ?? "";
    this.updateWorkflowProperty(
      this.workflowPersistService.updateWorkflowDescription.bind(this.workflowPersistService),
      "description",
      updatedDescription,
      this.originalDescription
    );
  }

  formatTime(timestamp: number | undefined): string {
    if (timestamp === undefined) {
      return "Unknown"; // Return "Unknown" if the timestamp is undefined
    }

    const currentTime = new Date().getTime();
    const timeDifference = currentTime - timestamp;

    const minutesAgo = Math.floor(timeDifference / (1000 * 60));
    const hoursAgo = Math.floor(timeDifference / (1000 * 60 * 60));
    const daysAgo = Math.floor(timeDifference / (1000 * 60 * 60 * 24));
    const weeksAgo = Math.floor(daysAgo / 7);

    if (minutesAgo < 60) {
      return `${minutesAgo} minutes ago`;
    } else if (hoursAgo < 24) {
      return `${hoursAgo} hours ago`;
    } else if (daysAgo < 7) {
      return `${daysAgo} days ago`;
    } else if (weeksAgo < 4) {
      return `${weeksAgo} weeks ago`;
    } else {
      return new Date(timestamp).toLocaleDateString();
    }
  }

  openDetailModal(wid: number | undefined): void {
    const modalRef = this.modal.create({
      nzTitle: "Workflow Detail",
      nzContent: HubWorkflowDetailComponent,
      nzFooter: null,
      nzStyle: { width: "60%" },
      nzBodyStyle: { maxHeight: "70vh", overflow: "auto" },
    });

    const instance = modalRef.componentInstance;
    if (instance) {
      if (wid !== undefined) {
        instance.wid = wid;
      } else {
        console.warn("wid is undefined, default handling can be added here");
        instance.wid = 0;
      }
    }
  }

  toggleLike(workflowId: number | undefined, userId: number | undefined): void {
    if (workflowId === undefined || userId === undefined) {
      return;
    }

    if (this.isLiked) {
      this.hubWorkflowService
        .postUnlikeWorkflow(workflowId, userId)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = false;
            this.hubWorkflowService
              .getLikeCount(workflowId)
              .pipe(untilDestroyed(this))
              .subscribe((count: number) => {
                this.likeCount = count;
              });
            console.log("Successfully unliked the workflow");
          } else {
            console.error("Error unliking the workflow");
          }
        });
    } else {
      this.hubWorkflowService
        .postLikeWorkflow(workflowId, userId)
        .pipe(untilDestroyed(this))
        .subscribe((success: boolean) => {
          if (success) {
            this.isLiked = true;
            this.hubWorkflowService
              .getLikeCount(workflowId)
              .pipe(untilDestroyed(this))
              .subscribe((count: number) => {
                this.likeCount = count;
              });
            console.log("Successfully liked the workflow");
          } else {
            console.error("Error liking the workflow");
          }
        });
    }
  }

  formatLikeCount(count: number): string {
    if (count >= 1000) {
      return (count / 1000).toFixed(1) + "k";
    }
    return count.toString();
  }

  // alias for formatSize
  formatSize = formatSize;
}
