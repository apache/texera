import { Component, inject, OnInit } from "@angular/core";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DatasetFileNode } from "../../../common/type/datasetVersionFileTree";
import { DatasetVersion } from "../../../common/type/dataset";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";

@UntilDestroy()
@Component({
  selector: "texera-directory-selection-modal",
  templateUrl: "directory-selection.component.html",
  styleUrls: ["directory-selection.component.scss"],
})
export class DirectorySelectionComponent {
  readonly datasets: ReadonlyArray<DashboardDataset> = inject(NZ_MODAL_DATA).datasets;
  readonly selectedDirectoryPath: string = inject(NZ_MODAL_DATA).selectedDirectoryPath;

  selectedDataset?: DashboardDataset;
  selectedVersion?: DatasetVersion;
  datasetVersions?: DatasetVersion[];
  suggestedFileTreeNodes: DatasetFileNode[] = [];
  isDatasetSelected: boolean = false;

  constructor(
    private modalRef: NzModalRef,
    private datasetService: DatasetService
  ) {}

  ngOnInit() {
    // Load initial dataset and directory
    if (this.selectedDirectoryPath && this.selectedDirectoryPath !== "") {
      this.datasetService
        .retrieveAccessibleDatasets(false, false, this.selectedDirectoryPath)
        .pipe(untilDestroyed(this))
        .subscribe(response => {
          const prevDataset = response.datasets[0];
          this.selectedDataset = this.datasets.find(d => d.dataset.did === prevDataset.dataset.did);
          this.isDatasetSelected = !!this.selectedDataset;

          if (this.selectedDataset && this.selectedDataset.dataset.did !== undefined) {
            this.datasetService
              .retrieveDatasetVersionList(this.selectedDataset.dataset.did)
              .pipe(untilDestroyed(this))
              .subscribe(versions => {
                this.datasetVersions = versions;
                const versionDvid = prevDataset.versions[0].datasetVersion.dvid;
                this.selectedVersion = this.datasetVersions.find(v => v.dvid === versionDvid);
                this.onVersionChange();
              });
          }
        });
    }
  }

  onDatasetChange() {
    this.selectedVersion = undefined;
    this.suggestedFileTreeNodes = [];
    this.isDatasetSelected = !!this.selectedDataset;
    if (this.selectedDataset && this.selectedDataset.dataset.did !== undefined) {
      this.datasetService
        .retrieveDatasetVersionList(this.selectedDataset.dataset.did)
        .pipe(untilDestroyed(this))
        .subscribe(versions => {
          this.datasetVersions = versions;
          if (this.datasetVersions && this.datasetVersions.length > 0) {
            this.selectedVersion = this.datasetVersions[0];
            this.onVersionChange();
          }
        });
    }
  }

  onVersionChange() {
    this.suggestedFileTreeNodes = [];
    if (
      this.selectedDataset &&
      this.selectedDataset.dataset.did !== undefined &&
      this.selectedVersion &&
      this.selectedVersion.dvid !== undefined
    ) {
      this.datasetService
        .retrieveDatasetVersionFileTree(this.selectedDataset.dataset.did, this.selectedVersion.dvid)
        .pipe(untilDestroyed(this))
        .subscribe(fileNodes => {
          this.suggestedFileTreeNodes = fileNodes;
        });
    }
  }

  onSelectVersion() {
    if (this.selectedVersion && this.selectedDataset) {
      this.modalRef.close(
        `${this.selectedDataset.ownerEmail}/${this.selectedDataset.dataset.name}/${this.selectedVersion.name}`
      );
    }
  }
}
