import { Component } from "@angular/core";
import {AppSettings} from "../../../../common/app-setting";
import { differenceInSeconds, parseISO } from "date-fns";
import { Clusters } from "../../../type/clusters";
import { ClusterService } from "../../../../common/service/cluster/cluster.service";
import { Subscription } from "rxjs";

@Component({
  selector: "texera-cluster",
  templateUrl: "./cluster.component.html",
  styleUrls: ["./cluster.component.css"]
})
export class ClusterComponent {
  private subscriptions: Subscription[] = [];
  private intervalId: any;
  isClusterManagementVisible = false;
  clusterList: Clusters[] = [];

  constructor(
    private clusterService: ClusterService,
  ) {}

  ngOnInit(): void{
    this.startClusterPolling();
  }

  private startClusterPolling(): void {
    this.intervalId = setInterval(() => {
      this.clusterService.getClusters()
        .subscribe({next: (response) => {
            this.clusterList = response || [];
          }});
    }, 1000);
  }

  private stopClusterPolling(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
    }
  }

  deleteCluster(cluster: Clusters): void{
    this.clusterService.deleteCluster(cluster)
      .subscribe({next: (response) => {
        console.log("Successful: ", response);
      }});
  }

  openClusterManagementModal(): void {
    this.isClusterManagementVisible = true;
  }

  closeClusterManagementModal(): void {
    this.isClusterManagementVisible = false;
  }
}
