import { Component, inject, OnInit } from "@angular/core";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { DatasetFileNode } from "../../../common/type/datasetVersionFileTree";
import { DatasetVersion } from "../../../common/type/dataset";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { parseFilePathToDatasetFile } from "../../../common/type/dataset-file";

@UntilDestroy()
@Component({
  selector: "texera-file-selection-model",
  templateUrl: "file-selection.component.html",
  styleUrls: ["file-selection.component.scss"],
})
export class FileSelectionComponent implements OnInit {
  readonly datasets: ReadonlyArray<DashboardDataset> = inject(NZ_MODAL_DATA).datasets;
  readonly selectedFilePath: string = inject(NZ_MODAL_DATA).selectedFilePath;

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
    // if users already select some file, then show that selected dataset & related version
    if (this.selectedFilePath && this.selectedFilePath !== "") {
      this.datasetService
        .retrieveAccessibleDatasets(false, false, this.selectedFilePath)
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

  onFileTreeNodeSelected(node: DatasetFileNode) {
    this.modalRef.close(node);
  }
}
