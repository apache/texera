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
  createdAtTooltip: string = "";

  constructor(private modalRef: NzModalRef) {}

  ngOnInit(): void {
    // Initialize with the first cluster if available
    if (this.clusters.length > 0) {
      this.selectedCluster = this.clusters[0];
      this.isClusterSelected = true;
      this.updateCreatedAtTooltip();
    }
  }

  onClusterChange() {
    this.isClusterSelected = !!this.selectedCluster;
    this.updateCreatedAtTooltip();
  }

  updateCreatedAtTooltip() {
    if (this.selectedCluster) {
      const date = new Date(this.selectedCluster.creationTime);
      this.createdAtTooltip = `${date.toLocaleString()} ${this.getTimeZone(date)}`;
    }
  }

  getTimeZone(date: Date): string {
    const offset = -date.getTimezoneOffset();
    const sign = offset >= 0 ? "+" : "-";
    const hours = String(Math.floor(Math.abs(offset) / 60)).padStart(2, "0");
    const minutes = String(Math.abs(offset) % 60).padStart(2, "0");
    return `GMT${sign}${hours}:${minutes}`;
  }

  onConfirm() {
    this.modalRef.close(this.selectedCluster);
  }

  onCancel() {
    this.modalRef.close();
  }
}
