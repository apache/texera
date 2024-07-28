import { Component, inject, OnInit } from "@angular/core";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { UntilDestroy } from "@ngneat/until-destroy";
import { DatasetFileNode } from "../../../common/type/datasetVersionFileTree";
import { DatasetVersion } from "../../../common/type/dataset";
import { DashboardDataset } from "../../../dashboard/type/dashboard-dataset.interface";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";

@UntilDestroy()
@Component({
  selector: "texera-file-selection-model",
  templateUrl: "file-selection.component.html",
  styleUrls: ["file-selection.component.scss"],
})
export class FileSelectionComponent implements OnInit {
  readonly datasets: ReadonlyArray<DashboardDataset> = inject(NZ_MODAL_DATA).datasets; // 获取传递的数据集信息
  selectedDataset?: DashboardDataset;
  selectedVersion?: DatasetVersion;
  datasetVersions?: DatasetVersion[];
  suggestedFileTreeNodes: DatasetFileNode[] = [];

  constructor(private modalRef: NzModalRef, private datasetService: DatasetService) {}

  ngOnInit() {}

  onDatasetChange() {
    this.selectedVersion = undefined;
    this.suggestedFileTreeNodes = [];
    if (this.selectedDataset && this.selectedDataset.dataset.did !== undefined) {
      this.datasetService.retrieveDatasetVersionList(this.selectedDataset.dataset.did).subscribe((versions) => {
        this.datasetVersions = versions;
        // set default version to the latest version
        if (this.datasetVersions && this.datasetVersions.length > 0) {
          this.selectedVersion = this.datasetVersions[0];
          this.onVersionChange(); 
        }
      });
    }
  }

  onVersionChange() {
    this.suggestedFileTreeNodes = [];
    if (this.selectedDataset && this.selectedDataset.dataset.did !== undefined && this.selectedVersion && this.selectedVersion.dvid !== undefined) {
      this.datasetService
        .retrieveDatasetVersionFileTree(this.selectedDataset.dataset.did, this.selectedVersion.dvid)
        .subscribe(fileNodes => {
          this.suggestedFileTreeNodes = fileNodes;
          console.log("+++++")
          console.log(this.suggestedFileTreeNodes)
        });
    }
  }

  onFileTreeNodeSelected(node: DatasetFileNode) {
    this.modalRef.close(node);
  }
}
