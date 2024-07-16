import { EnvironmentService } from "../../../../dashboard/service/user/environment/environment.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, Input, OnInit } from "@angular/core";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import {
  DatasetVersionFileTreeNode,
  getFullPathFromFileTreeNode,
} from "../../../../common/type/datasetVersionFileTree";
import { DatasetService } from "../../../../dashboard/service/user/dataset/dataset.service";
import { DashboardDataset } from "../../../../dashboard/type/dashboard-dataset.interface";
import { DatasetOfEnvironmentDetails, Environment } from "../../../../common/type/environment";
import { DatasetVersion } from "../../../../common/type/dataset";
import { map, Observable, of } from "rxjs";
import { ActivatedRoute } from "@angular/router";

@UntilDestroy()
@Component({
  selector: "texera-environment",
  templateUrl: "environment.component.html",
  styleUrls: ["environment.component.scss"],
})
export class EnvironmentComponent implements OnInit {
  @Input()
  eid: number | undefined;

  wid: number | undefined;

  selectedMenu: "datasets" = "datasets";

  environment: Environment | undefined;

  // [did] => [DatasetOfEnvironmentDetails, DatasetVersionFileTreeNode[]]
  datasetsOfEnvironment: Map<number, [DatasetOfEnvironmentDetails, DatasetVersionFileTreeNode[]]> = new Map();

  // [did, datasetName, DatasetVersionFileTreeNode[]]
  datasetFileTrees: [number, string, DatasetVersionFileTreeNode[]][] = [];

  // dataset link related control
  showDatasetLinkModal: boolean = false;
  userAccessibleDatasets: DashboardDataset[] = [];
  filteredLinkingDatasets: { did: number | undefined; name: string }[] = [];
  inputDatasetName?: string;

  // dataset details related control
  showDatasetDetails: boolean = false;
  showingDataset: DatasetOfEnvironmentDetails | undefined;
  showingDatasetVersions: DatasetVersion[] = [];
  selectedShowingDatasetVersion: DatasetVersion | undefined;

  // dataset file display related control
  showDatasetFile: boolean = false;
  showingDatasetFile: DatasetVersionFileTreeNode | undefined;
  showingDatasetFileDid: number | undefined;
  showingDatasetFileDvid: number | undefined;

  isLoading: boolean = false;

  constructor(
    private environmentService: EnvironmentService,
    private notificationService: NotificationService,
    private workflowPersistService: WorkflowPersistService,
    private workflowActionService: WorkflowActionService,
    private datasetService: DatasetService,
    private route: ActivatedRoute
  ) {}

  ngOnInit(): void {
    this.isLoading = true;
    this.wid = this.route.snapshot.params.id;
    if (this.wid) {
      this.loadEnvironment();
    } else {
      this.isLoading = false;
      this.notificationService.error("Encounter issues when loading the environment of current workflow");
    }
  }

  private loadEnvironment() {
    if (this.wid) {
      this.workflowPersistService
        .retrieveWorkflowEnvironment(this.wid)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: env => {
            this.isLoading = false;
            this.environment = env;
            this.eid = env.eid;
            this.loadDatasetsOfEnvironment();
          },
          error: (err: unknown) => {
            this.isLoading = false;
            this.notificationService.warning(`Runtime environment of current workflow not found.
                          Please save current workflow, so that the environment will be created automatically.`);
          },
        });
    }
  }

  private loadDatasetsOfEnvironment() {
    this.datasetFileTrees = [];
    this.datasetsOfEnvironment = new Map();
    if (this.eid) {
      const eid = this.eid;
      this.environmentService
        .retrieveDatasetsOfEnvironmentDetails(eid)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: datasets => {
            datasets.forEach(entry => {
              const did = entry.dataset.did;
              const dvid = entry.version.dvid;
              if (did && dvid) {
                this.datasetService
                  .retrieveDatasetVersionFileTree(did, dvid)
                  .pipe(untilDestroyed(this))
                  .subscribe({
                    next: datasetFileTree => {
                      this.datasetsOfEnvironment.set(did, [entry, datasetFileTree]);
                      this.datasetFileTrees.push([did, entry.dataset.name, datasetFileTree]);
                    },
                  });
              }
            });
          },
          error: (err: unknown) => {
            this.notificationService.error("Datasets of Environment loading error!");
          },
        });
    }
  }

  onClickOpenEnvironmentDatasetDetails(did: number) {
    const selectedEntry = this.datasetsOfEnvironment.get(did);
    if (selectedEntry) {
      this.showingDataset = selectedEntry[0];
      this.datasetService
        .retrieveDatasetVersionList(Number(this.showingDatasetDid))
        .pipe(untilDestroyed(this))
        .subscribe(versions => {
          this.showingDatasetVersions = versions;
          this.selectedShowingDatasetVersion = this.showingDatasetVersions.find((version, i, versions) => {
            return versions[i].dvid == this.showingDataset?.version.dvid;
          });
          this.showDatasetDetails = true;
        });
    }
  }

  // related control for dataset link modal
  onClickOpenDatasetAddModal() {
    // initialize the datasets info
    this.datasetService
      .retrieveAccessibleDatasets()
      .pipe(untilDestroyed(this))
      .subscribe({
        next: datasets => {
          this.userAccessibleDatasets = datasets.filter(ds => {
            const newDid = ds.dataset.did;
            const newName = ds.dataset.name;

            // Check if the datasetsOfEnvironment does not have the newDid
            const didNotExist = newDid && !this.datasetsOfEnvironment.has(newDid);

            // Check if the datasetsOfEnvironment does not have the newName
            const nameNotExist = ![...this.datasetsOfEnvironment.values()].some(
              ([details, _]) => details.dataset.name === newName
            );
            return didNotExist && nameNotExist;
          });

          this.filteredLinkingDatasets = this.userAccessibleDatasets.map(dataset => ({
            name: dataset.dataset.name,
            did: dataset.dataset.did,
          }));

          if (this.userAccessibleDatasets.length == 0) {
            this.notificationService.warning("There is no available datasets to be added to the environment.");
          } else {
            this.showDatasetLinkModal = true;
          }
        },
      });
  }

  handleVersionChange(newVersion: DatasetVersion) {
    const previousVersion = this.selectedShowingDatasetVersion;
    if (this.eid && newVersion.dvid) {
      this.environmentService
        .updateDatasetVersionInEnvironment(this.eid, newVersion.did, newVersion.dvid)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.notificationService.success(
              `The workflow are now binding with Dataset ${this.showingDatasetName} Version ${newVersion.name}`
            );
            this.selectedShowingDatasetVersion = newVersion;
            this.loadDatasetsOfEnvironment();
          },
          error: (err: unknown) => {
            this.notificationService.error("Failed to bind with different version of the dataset.");
            this.selectedShowingDatasetVersion = previousVersion;
          },
        });
    }
  }
  handleCancelLinkDataset() {
    this.showDatasetLinkModal = false;
  }

  onClickAddDataset(dataset: { did: number | undefined; name: string }) {
    if (this.eid && dataset.did) {
      this.environmentService
        .addDatasetToEnvironment(this.eid, dataset.did)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: response => {
            this.notificationService.success(`Link dataset ${dataset.name} to the environment successfully`);
            this.showDatasetLinkModal = false;
            this.loadDatasetsOfEnvironment();
          },
          error: (err: unknown) => {
            this.notificationService.error(`Linking dataset ${dataset.name} encounters error`);
          },
        });
    }
  }

  onUserInputDatasetName(event: Event): void {
    const value = this.inputDatasetName;

    if (value) {
      this.filteredLinkingDatasets = this.userAccessibleDatasets
        .filter(dataset => !dataset.dataset.did || dataset.dataset.name.toLowerCase().includes(value))
        .map(dataset => ({
          name: dataset.dataset.name,
          did: dataset.dataset.did,
        }));
    }
  }

  // controls of dataset details
  get showingDatasetName(): string {
    if (this.showingDataset?.dataset.name) {
      return this.showingDataset.dataset.name;
    }

    return "";
  }

  get showingDatasetDid(): string {
    const did = this.showingDataset?.dataset.did;
    if (did) {
      return did.toString();
    }
    return "";
  }

  get showingDatasetVersionName(): string {
    const versionName = this.showingDataset?.version.name;
    if (versionName) {
      return versionName;
    }
    return "";
  }

  get showingDatasetVersion(): DatasetVersion {
    if (this.showingDataset?.version) {
      return this.showingDataset?.version;
    }
    return {
      did: 1,
      dvid: 1,
      creatorUid: 1,
      name: "",
      versionHash: "",
      creationTime: Date.now(),
      versionFileTreeNodes: undefined,
    };
  }

  get showingDatasetDescription(): string {
    const desc = this.showingDataset?.dataset.description;
    if (desc) {
      return desc;
    }
    return "";
  }

  handleCancelDatasetDetails() {
    this.showDatasetDetails = false;
  }

  // controls for displaying dataset file
  displayDatasetFileContent(node: DatasetVersionFileTreeNode, did: number) {
    const datasetDetails = this.datasetsOfEnvironment.get(did);
    if (datasetDetails) {
      this.showDatasetFile = true;
      this.showingDatasetFile = node;
      this.showingDatasetFileDid = did;
      this.showingDatasetFileDvid = datasetDetails[0].version.dvid;
    }
  }

  get selectedDatasetFileDid(): number {
    if (this.showingDatasetFileDid) {
      return this.showingDatasetFileDid;
    }
    return 0;
  }

  get selectedDatasetFileDvid(): number {
    if (this.showingDatasetFileDvid) {
      return this.showingDatasetFileDvid;
    }
    return 0;
  }

  get selectedDatasetFilename(): string {
    if (this.showingDatasetFile) {
      return getFullPathFromFileTreeNode(this.showingDatasetFile);
    }
    return "";
  }

  handleCancelDatasetFileDisplay() {
    this.showDatasetFile = false;
  }

  onConfirmRemoveDatasetFromEnvironment() {
    if (this.eid) {
      this.environmentService
        .removeDatasetFromEnvironment(this.eid, Number(this.showingDatasetDid))
        .pipe(untilDestroyed(this))
        .subscribe({
          next: res => {
            this.notificationService.success(
              `Dataset ${this.showingDatasetName} has been removed from current environment`
            );
            this.showDatasetDetails = false;
            this.loadDatasetsOfEnvironment();
          },
        });
    }
  }
}
