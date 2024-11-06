import { Component, OnInit } from "@angular/core";
import { ActivatedRoute, NavigationEnd, Router } from "@angular/router";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DatasetService } from "../../../../service/user/dataset/dataset.service";
import { NzResizeEvent } from "ng-zorro-antd/resizable";
import { DatasetFileNode, getFullPathFromDatasetFileNode } from "../../../../../common/type/datasetVersionFileTree";
import { DatasetVersion } from "../../../../../common/type/dataset";
import { switchMap } from "rxjs/operators";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { DownloadService } from "../../../../service/user/download/download.service";
import { formatSize } from "../../../../../common/util/size-formatter.util";


@UntilDestroy()
@Component({
  templateUrl: "./user-dataset-explorer.component.html",
  styleUrls: ["./user-dataset-explorer.component.scss"],
})
export class UserDatasetExplorerComponent implements OnInit {
  public did: number | undefined;
  public datasetName: string = "";
  public datasetDescription: string = "";
  public datasetCreationTime: string = "";
  public datasetIsPublic: boolean = false;
  public userDatasetAccessLevel: "READ" | "WRITE" | "NONE" = "NONE";

  public currentDisplayedFileName: string = "";
  public currentFileSize: number | undefined;
  public currentDatasetVersionSize: number | undefined;

  public isRightBarCollapsed = false;
  public isMaximized = false;

  public versions: ReadonlyArray<DatasetVersion> = [];
  public selectedVersion: DatasetVersion | undefined;
  public fileTreeNodeList: DatasetFileNode[] = [];

  public isCreatingVersion: boolean = false;
  public isCreatingDataset: boolean = false;
  public versionCreatorBaseVersion: DatasetVersion | undefined;

  constructor(
    private route: ActivatedRoute,
    private router: Router,
    private datasetService: DatasetService,
    private notificationService: NotificationService,
    private downloadService: DownloadService
  ) {}

  // item for control the resizeable sider
  MAX_SIDER_WIDTH = 600;
  MIN_SIDER_WIDTH = 150;
  siderWidth = 200;
  id = -1;
  onSideResize({ width }: NzResizeEvent): void {
    cancelAnimationFrame(this.id);
    this.id = requestAnimationFrame(() => {
      this.siderWidth = width!;
    });
  }

  ngOnInit(): void {
    this.route.params
      .pipe(
        switchMap(params => {
          const param = params["did"];
          if (param !== "create") {
            this.did = param;
            this.renderDatasetViewSider();
            this.retrieveDatasetInfo();
            this.retrieveDatasetVersionList();
          } else {
            this.renderDatasetCreatorSider();
          }
          return this.route.data; // or some other observable
        }),
        untilDestroyed(this)
      )
      .subscribe();
  }

  renderDatasetViewSider() {
    this.isCreatingVersion = false;
    this.isCreatingDataset = false;
  }
  renderDatasetCreatorSider() {
    this.isCreatingVersion = false;
    this.isCreatingDataset = true;
    this.siderWidth = this.MAX_SIDER_WIDTH;
  }

  renderVersionCreatorSider() {
    if (this.did) {
      this.datasetService
        .retrieveDatasetLatestVersion(this.did)
        .pipe(untilDestroyed(this))
        .subscribe(latestVersion => {
          this.versionCreatorBaseVersion = latestVersion;
          this.isCreatingDataset = false;
          this.isCreatingVersion = true;
          this.siderWidth = this.MAX_SIDER_WIDTH;
        });
    }
  }

  public onCreationFinished(creationID: number) {
    if (creationID != 0) {
      // creation succeed
      if (this.isCreatingVersion) {
        this.retrieveDatasetVersionList();
        this.renderDatasetViewSider();
      } else {
        this.router.navigate([`/dashboard/user/dataset/${creationID}`]);
      }
    } else {
      // creation failed
      if (this.isCreatingVersion) {
        this.isCreatingVersion = false;
        this.isCreatingDataset = false;
        this.retrieveDatasetVersionList();
      } else {
        this.router.navigate(["/dashboard/user/dataset"]);
      }
    }
  }

  public onClickOpenVersionCreator() {
    this.renderVersionCreatorSider();
  }

  onPublicStatusChange(checked: boolean): void {
    // Handle the change in dataset public status
    if (this.did) {
      this.datasetService
        .updateDatasetPublicity(this.did)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.datasetIsPublic = checked;
            let state = "public";
            if (!this.datasetIsPublic) {
              state = "private";
            }
            this.notificationService.success(`Dataset ${this.datasetName} is now ${state}`);
          },
          error: (err: unknown) => {
            this.notificationService.error("Fail to change the dataset publicity");
          },
        });
    }
  }

  retrieveDatasetInfo() {
    if (this.did) {
      this.datasetService
        .getDataset(this.did)
        .pipe(untilDestroyed(this))
        .subscribe(dashboardDataset => {
          const dataset = dashboardDataset.dataset;
          this.datasetName = dataset.name;
          this.datasetDescription = dataset.description;
          this.userDatasetAccessLevel = dashboardDataset.accessPrivilege;
          this.datasetIsPublic = dataset.isPublic === 1;
          if (typeof dataset.creationTime === "number") {
            this.datasetCreationTime = new Date(dataset.creationTime).toString();
          }
        });
    }
  }

  retrieveDatasetVersionList() {
    if (this.did) {
      this.datasetService
        .retrieveDatasetVersionList(this.did)
        .pipe(untilDestroyed(this))
        .subscribe(versionNames => {
          this.versions = versionNames;
          // by default, the selected version is the 1st element in the retrieved list
          // which is guaranteed(by the backend) to be the latest created version.
          this.selectedVersion = this.versions[0];
          this.onVersionSelected(this.selectedVersion);
        });
    }
  }

  loadFileContent(node: DatasetFileNode) {
    this.currentDisplayedFileName = getFullPathFromDatasetFileNode(node);
    this.currentFileSize = node.size;
  }

  onClickDownloadCurrentFile = (): void => {
    if (!this.did || !this.selectedVersion?.dvid) return;

    this.downloadService.downloadSingleFile(this.currentDisplayedFileName).pipe(untilDestroyed(this)).subscribe();
  };

  onClickDownloadVersionAsZip = (): void => {
    if (!this.did || !this.selectedVersion?.dvid) return;

    this.downloadService
      .downloadDatasetVersion(this.did, this.selectedVersion.dvid, this.datasetName, this.selectedVersion.name)
      .pipe(untilDestroyed(this))
      .subscribe();
  };

  onClickScaleTheView() {
    this.isMaximized = !this.isMaximized;
  }

  onClickHideRightBar() {
    this.isRightBarCollapsed = !this.isRightBarCollapsed;
  }

  onVersionSelected(version: DatasetVersion): void {
    this.selectedVersion = version;
    if (this.did && this.selectedVersion.dvid)
      this.datasetService
        .retrieveDatasetVersionFileTree(this.did, this.selectedVersion.dvid)
        .pipe(untilDestroyed(this))
        .subscribe(data => {
          this.fileTreeNodeList = data.fileNodes;
          this.currentDatasetVersionSize = data.size;
          let currentNode = this.fileTreeNodeList[0];
          while (currentNode.type === "directory" && currentNode.children) {
            currentNode = currentNode.children[0];
          }
          this.loadFileContent(currentNode);
        });
  }

  onVersionFileTreeNodeSelected(node: DatasetFileNode) {
    this.loadFileContent(node);
  }

  isDisplayingDataset(): boolean {
    return !this.isCreatingDataset && !this.isCreatingVersion;
  }

  userHasWriteAccess(): boolean {
    return this.userDatasetAccessLevel == "WRITE";
  }

  // alias for formatSize
  formatSize = formatSize;
}
