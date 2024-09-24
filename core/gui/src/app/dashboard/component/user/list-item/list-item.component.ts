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
} from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { ShareAccessComponent } from "../share-access/share-access.component";
import {
  WorkflowPersistService,
  DEFAULT_WORKFLOW_NAME,
} from "src/app/common/service/workflow-persist/workflow-persist.service";
import { Workflow } from "src/app/common/type/workflow";
import { FileSaverService } from "src/app/dashboard/service/user/file/file-saver.service";
import { firstValueFrom } from "rxjs";

@UntilDestroy()
@Component({
  selector: "texera-list-item",
  templateUrl: "./list-item.component.html",
  styleUrls: ["./list-item.component.scss"],
})
export class ListItemComponent implements OnInit, OnChanges {
  @ViewChild("nameInput") nameInput!: ElementRef;
  @ViewChild("descriptionInput") descriptionInput!: ElementRef;
  editingName = false;
  editingDescription = false;

  ROUTER_WORKFLOW_BASE_URL = "/dashboard/user/workspace";
  ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user/project";
  ROUTER_DATASET_BASE_URL = "/dashboard/user/dataset";
  public entryLink: string = "";
  public iconType: string = "";
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
    private modalService: NzModalService,
    private workflowPersistService: WorkflowPersistService,
    private fileSaverService: FileSaverService
  ) {}

  initializeEntry() {
    if (this.entry.type === "workflow") {
      this.entryLink = this.ROUTER_WORKFLOW_BASE_URL + "/" + this.entry.id;
      this.iconType = "project";
    } else if (this.entry.type === "project") {
      this.entryLink = this.ROUTER_USER_PROJECT_BASE_URL + "/" + this.entry.id;
      this.iconType = "container";
    } else if (this.entry.type === "dataset") {
      this.entryLink = this.ROUTER_DATASET_BASE_URL + "/" + this.entry.id;
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
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes["entry"]) {
      this.initializeEntry();
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
        },
        nzFooter: null,
        nzTitle: "Share this workflow with others",
        nzCentered: true,
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
      });
    }
  }

  public onClickDownload(): void {
    if (this.entry.type === "workflow") {
      if (this.entry.id) {
        this.workflowPersistService
          .retrieveWorkflow(this.entry.id)
          .pipe(untilDestroyed(this))
          .subscribe(data => {
            const workflowCopy: Workflow = {
              ...data,
              wid: undefined,
              creationTime: undefined,
              lastModifiedTime: undefined,
              readonly: false,
            };
            const workflowJson = JSON.stringify(workflowCopy.content);
            const fileName = workflowCopy.name + ".json";
            this.fileSaverService.saveAs(new Blob([workflowJson], { type: "text/plain;charset=utf-8" }), fileName);
          });
      }
    }
  }

  onEditName(): void {
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

  public confirmUpdateWorkflowCustomName(name: string): void {
    this.workflowPersistService
      .updateWorkflowName(this.entry.id, name || DEFAULT_WORKFLOW_NAME)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.entry.name = name || DEFAULT_WORKFLOW_NAME;
      })
      .add(() => {
        this.editingName = false;
      });
  }

  public confirmUpdateWorkflowCustomDescription(description: string | undefined): void {
    const updatedDescription = description !== undefined ? description : "";

    this.workflowPersistService
      .updateWorkflowDescription(this.entry.id, updatedDescription)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.entry.description = updatedDescription;
      })
      .add(() => {
        this.editingDescription = false;
      });
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
}
