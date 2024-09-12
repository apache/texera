import { Component } from "@angular/core";
import { FieldType, FieldTypeConfig } from "@ngx-formly/core";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { ClusterSelectionComponent } from "../cluster-selection/cluster-selection.component";
import { ClusterService } from "src/app/common/service/cluster/cluster.service";
import { environment } from "src/environments/environment";

@UntilDestroy()
@Component({
  selector: "texera-cluster-autocomplete-template",
  templateUrl: "./cluster-autocomplete.component.html",
  styleUrls: ["cluster-autocomplete.component.scss"],
})
export class ClusterAutoCompleteComponent extends FieldType<FieldTypeConfig> {
  constructor(
    private modalService: NzModalService,
    private clusterService: ClusterService
  ) {
    super();
  }

  onClickOpenClusterSelectionModal(): void {
    this.clusterService
      .getClusters(true)
      .pipe(untilDestroyed(this))
      .subscribe(clusters => {
        const modal = this.modalService.create({
          nzTitle: "Select Cluster",
          nzContent: ClusterSelectionComponent,
          nzData: { clusters: clusters },
          nzFooter: null,
        });

        modal.afterClose.pipe(untilDestroyed(this)).subscribe(selectedCluster => {
          if (selectedCluster) {
            this.formControl.setValue(String(selectedCluster.cid));
          }
        });
      });
  }
  get isClusterSelectionEnabled(): boolean {
    return environment.userSystemEnabled;
  }
}
