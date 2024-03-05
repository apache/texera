import { EnvironmentService } from "../../../../dashboard/user/service/user-environment/environment.service";
import { NotificationService } from "../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { WorkflowPersistService } from "../../../../common/service/workflow-persist/workflow-persist.service";
import { WorkflowActionService } from "../../../service/workflow-graph/model/workflow-action.service";
import {
  DatasetVersionFileTreeNode,
  getFullPathFromFileTreeNode,
} from "../../../../common/type/datasetVersionFileTree";
import { DatasetService } from "../../../../dashboard/user/service/user-dataset/dataset.service";
import { DashboardDataset } from "../../../../dashboard/user/type/dashboard-dataset.interface";
import {DatasetOfEnvironmentDetails, Environment} from "../../../../common/type/environment";

@UntilDestroy()
@Component({
  selector: "texera-environment",
  templateUrl: "environment.component.html",
  styleUrls: ["environment.component.scss"],
})
export class EnvironmentComponent implements OnInit {
  // this input is for other components,
  // e.g. environment viewer, that already have the eid to use
  @Input()
  eid: number | undefined;

  wid: number | undefined;

  selectedMenu: "datasets" = "datasets";

  environment: Environment | undefined;
  environmentTooltip: string =
    "Environment manages the workflow related information, including the datasets visible to current workflow.\n";

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

  // dataset file display related control
  showDatasetFile: boolean = false;
  showingDatasetFile: DatasetVersionFileTreeNode | undefined;
  showingDatasetFileDid: number | undefined;
  showingDatasetFileDvid: number | undefined;

  constructor(
    private environmentService: EnvironmentService,
    private notificationService: NotificationService,
    private workflowPersistService: WorkflowPersistService,
    private workflowActionService: WorkflowActionService,
    private datasetService: DatasetService,
  ) {}

  ngOnInit(): void {
  // initialize the environment info
    this.wid = this.workflowActionService.getWorkflowMetadata()?.wid;
    if (this.wid) {
      // use wid to fetch the eid first
      this.workflowPersistService
        .retrieveWorkflowEnvironment(this.wid)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: env => {
            this.environment = env;
            this.eid = env.eid;
            this.loadDatasetsOfEnvironment();
            this.setEnvironmentTooltip();
          },
          error: (err: unknown) => {
            this.notificationService.warning(`Runtime environment of current workflow not found.
                          Please save current workflow, so that the environment will be created automatically.`);
          },
        });
    }
  }

  private setEnvironmentTooltip() {
    if (this.environment) {
      this.environmentTooltip += `Name: ${this.environment.name}\n` + `Description: ${this.environment.description}\n`;
    }
  }

  private loadDatasetsOfEnvironment() {
    this.datasetFileTrees = [];
    if (this.eid) {
      const eid = this.eid;
      this.environmentService.retrieveDatasetsOfEnvironmentDetails(eid).subscribe({
        next: datasets => {
          datasets.forEach(entry => {
            const did = entry.dataset.did;
            const dvid = entry.version.dvid;
            if (did && dvid) {
              this.datasetService.retrieveDatasetVersionFileTree(did, dvid).subscribe({
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
      this.showDatasetDetails = true;
    }
  }

  // related control for dataset link modal
  onClickOpenDatasetAddModal() {
    // initialize the datasets info
    this.datasetService.retrieveAccessibleDatasets().subscribe({
      next: datasets => {
        console.log(datasets);
        this.userAccessibleDatasets = datasets.filter(ds => {
          const newDid = ds.dataset.did;
          return newDid && !this.datasetsOfEnvironment.has(newDid);
        });
        this.filteredLinkingDatasets = this.userAccessibleDatasets
          .map(dataset => ({
            name: dataset.dataset.name,
            did: dataset.dataset.did,
          }))

        if (this.userAccessibleDatasets.length == 0) {
          this.notificationService.warning("There is no available datasets to be added to the environment.");
        } else {
          this.showDatasetLinkModal = true;
        }
      },
    });
  }

  handleCancelLinkDataset() {
    this.showDatasetLinkModal = false;
  }

  onClickAddDataset(dataset: { did: number | undefined; name: string }) {
    if (this.eid && dataset.did) {
      this.environmentService.addDatasetToEnvironment(this.eid, dataset.did).subscribe({
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
    // console.log(this.filteredLinkingDatasetsName)
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
}
