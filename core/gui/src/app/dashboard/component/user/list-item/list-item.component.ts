import { Component, EventEmitter, Input, Output, OnInit, OnChanges, SimpleChanges } from "@angular/core";
import { environment } from "src/environments/environment";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { WorkflowExecutionHistoryComponent } from "../user-workflow/ngbd-modal-workflow-executions/workflow-execution-history.component";
import { DashboardEntry } from "src/app/dashboard/type/dashboard-entry";
import { ShareAccessComponent } from "../share-access/share-access.component";
import { DEFAULT_WORKFLOW_NAME, WorkflowPersistService } from "src/app/common/service/workflow-persist/workflow-persist.service";
import { Workflow } from "src/app/common/type/workflow";
import { Dataset } from "src/app/common/type/dataset";
import { DatasetService } from "src/app/dashboard/service/user/dataset/dataset.service";
import { FileSaverService } from "src/app/dashboard/service/user/file/file-saver.service";
import { DashboardProject } from "src/app/dashboard/type/dashboard-project.interface";
import { UserProjectService } from "src/app/dashboard/service/user/project/user-project.service";
import { firstValueFrom } from "rxjs";
import { DashboardDataset } from "src/app/dashboard/type/dashboard-dataset.interface";
import { NotificationService } from "src/app/common/service/notification/notification.service";

@UntilDestroy()
@Component({
  selector: 'texera-list-item',
  templateUrl: './list-item.component.html',
  styleUrls: ['./list-item.component.scss'],
})
export class ListItemComponent implements OnInit, OnChanges{
  ROUTER_WORKFLOW_BASE_URL = "/dashboard/workspace";
  ROUTER_USER_PROJECT_BASE_URL = "/dashboard/user-project";
  ROUTER_DATASET_BASE_URL = "/dashboard/dataset";
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
    private fileSaverService: FileSaverService,
    private userProjectService: UserProjectService,
    private datasetService: DatasetService,
    private notificationService: NotificationService
  ) {}

  initializeEntry() {
    if (this.entry.type === "workflow") {
      this.entryLink = this.ROUTER_WORKFLOW_BASE_URL + "/" + this.entry.id;
      this.iconType = "project";
    }
    else if (this.entry.type === "project") {
      this.entryLink = this.ROUTER_USER_PROJECT_BASE_URL + '/' + this.entry.id;
      this.iconType = "container";
    }
    else if (this.entry.type === "dataset") {
      this.entryLink = this.ROUTER_DATASET_BASE_URL + '/' + this.entry.id;
      this.iconType = "database";
    }
    else if (this.entry.type === "file") {
      // not sure where to redirect
      this.iconType = "folder-open";
    }
    else {
      throw new Error("Unexpected type in DashboardEntry.");
    }
  }

  ngOnInit(): void {
    this.initializeEntry();
  }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes['entry']) {
      this.initializeEntry()
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
    }
    else if (this.entry.type === "dataset") {
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
}