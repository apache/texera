import { UntilDestroy } from "@ngneat/until-destroy";
import { Component, inject, OnInit } from "@angular/core";
import { NZ_MODAL_DATA, NzModalRef } from "ng-zorro-antd/modal";
import { Clusters } from "src/app/dashboard/type/clusters";

@UntilDestroy()
@Component({
  selector: "texera-cluster-selection",
  templateUrl: "cluster-selection.component.html",
  styleUrls: ["cluster-selection.component.scss"],
})
export class ClusterSelectionComponent implements OnInit {
  readonly clusters: ReadonlyArray<Clusters> = inject(NZ_MODAL_DATA).clusters;
  selectedCluster?: Clusters;
  isClusterSelected: boolean = false;

  constructor(private modalRef: NzModalRef) {}

  ngOnInit(): void {
    // Initialize with the first cluster if available
    if (this.clusters.length > 0) {
      this.selectedCluster = this.clusters[0];
      this.isClusterSelected = true;
    }
  }

  onClusterChange() {
    this.isClusterSelected = !!this.selectedCluster;
  }

  onConfirm() {
    this.modalRef.close(this.selectedCluster);
  }

  onCancel() {
    this.modalRef.close(this.selectedCluster);
  }
}
