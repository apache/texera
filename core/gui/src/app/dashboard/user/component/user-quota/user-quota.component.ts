import { Component, inject } from "@angular/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { File, Workflow, MongoExecution, MongoWorkflow } from "../../../../common/type/user";
import { UserFileService } from "../../service/user-file/user-file.service";
import { NzTableSortFn } from "ng-zorro-antd/table";
import { UserQuotaService } from "../../service/user-quota/user-quota.service";
import { AdminUserService } from "../../../admin/service/admin-user.service";
import { NZ_MODAL_DATA } from "ng-zorro-antd/modal";

type UserServiceType = AdminUserService | UserQuotaService;

@UntilDestroy()
@Component({
  templateUrl: "./user-quota.component.html",
})
export class UserQuotaComponent {
  readonly userId: number;
  backgroundColor: String = "white";
  textColor: String = "Black";
  dynamicHeight: string = "700px";

  totalFileSize: number = 0;
  totalMongoSize: number = 0;
  createdFiles: ReadonlyArray<File> = [];
  createdWorkflows: ReadonlyArray<Workflow> = [];
  accessFiles: ReadonlyArray<number> = [];
  accessWorkflows: ReadonlyArray<number> = [];
  topFiveFiles: ReadonlyArray<File> = [];
  mongodbExecutions: ReadonlyArray<MongoExecution> = [];
  mongodbWorkflows: Array<MongoWorkflow> = [];
  UserService: UserServiceType;

  timer = setInterval(() => {}, 1000); // 1 second interval

  constructor(
    private adminUserService: AdminUserService,
    private userFileService: UserFileService,
    private regularUserService: UserQuotaService
  ) {
    this.UserService = adminUserService;
    if (inject(NZ_MODAL_DATA, { optional: true })) {
      this.userId = inject(NZ_MODAL_DATA).uid;
      this.UserService = this.adminUserService;
      this.backgroundColor = "lightcoral";
      this.textColor = "white";
    } else {
      this.userId = -1;
      this.UserService = this.regularUserService;
      this.dynamicHeight = "";
    }
    this.refreshData();
  }

  refreshData() {
    this.UserService.getUploadedFiles(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(fileList => {
        this.createdFiles = fileList;
        let size = 0;
        this.createdFiles.forEach(file => {
          size += file.fileSize;
        });
        this.totalFileSize = size;

        const copiedFiles = [...fileList];
        copiedFiles.sort((a, b) => b.fileSize - a.fileSize);
        this.topFiveFiles = copiedFiles.slice(0, 5);
      });

    this.UserService.getCreatedWorkflows(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(workflowList => {
        this.createdWorkflows = workflowList;
      });

    this.UserService.getAccessFiles(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(accessFiles => {
        this.accessFiles = accessFiles;
      });

    this.UserService.getAccessWorkflows(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(accessWorkflows => {
        this.accessWorkflows = accessWorkflows;
      });

    this.UserService.getMongoDBs(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(mongoList => {
        this.totalMongoSize = 0;
        this.mongodbExecutions = mongoList;
        this.mongodbWorkflows = [];

        this.mongodbExecutions.forEach(execution => {
          let insert = false;
          this.totalMongoSize += execution.size;

          this.mongodbWorkflows.some((workflow, index, array) => {
            if (workflow.workflowName === execution.workflowName) {
              array[index].executions.push(execution);
              insert = true;
              return;
            }
          });

          if (!insert) {
            let workflow: MongoWorkflow = {
              workflowName: execution.workflowName,
              executions: [] as MongoExecution[],
            };
            workflow.executions.push(execution);
            this.mongodbWorkflows.push(workflow);
          }
        });
      });
  }

  deleteMongoCollection(collectionName: string, execution: MongoExecution, workflowName: string) {
    this.UserService.deleteMongoDBCollection(collectionName)
      .pipe(untilDestroyed(this))
      .subscribe(() => {
        this.mongodbWorkflows.some((workflow, index, array) => {
          if (workflow.workflowName === workflowName) {
            array[index].executions = array[index].executions.filter(e => e !== execution);
            this.totalMongoSize -= execution.size;
          }
        });
      });
  }

  /**
   * Convert a numeric timestamp to a human-readable time string.
   */
  convertTimeToTimestamp(timeValue: number): string {
    const date = new Date(timeValue);
    return date.toLocaleString("en-US", { timeZoneName: "short" });
  }

  deleteFile(fid: number) {
    if (fid === undefined) {
      return;
    }
    this.userFileService
      .deleteFile(fid)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.refreshFiles());
  }

  downloadFile(fid: number, fileName: string) {
    this.userFileService
      .downloadFile(fid)
      .pipe(untilDestroyed(this))
      .subscribe((response: Blob) => {
        const link = document.createElement("a");
        link.download = fileName;
        link.href = URL.createObjectURL(new Blob([response]));
        link.click();
      });
  }

  convertFileSize(sizeInBytes: number): string {
    const units = ["B", "KB", "MB", "GB", "TB"];

    let size = sizeInBytes;
    let unitIndex = 0;

    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }

    return `${size.toFixed(2)} ${units[unitIndex]}`;
  }

  refreshFiles() {
    this.UserService.getUploadedFiles(this.userId)
      .pipe(untilDestroyed(this))
      .subscribe(fileList => {
        this.createdFiles = fileList;
        let size = 0;
        this.createdFiles.forEach(file => {
          size += file.fileSize;
        });
        this.totalFileSize = size;

        const copiedFiles = [...fileList];
        copiedFiles.sort((a, b) => b.fileSize - a.fileSize);
        this.topFiveFiles = copiedFiles.slice(0, 5);
      });
  }

  maxStringLength(input: string, length: number): string {
    if (input.length > length) {
      return input.substring(0, length) + " . . . ";
    }
    return input;
  }

  public sortByMongoDBSize: NzTableSortFn<MongoExecution> = (a: MongoExecution, b: MongoExecution) => b.size - a.size;
}
