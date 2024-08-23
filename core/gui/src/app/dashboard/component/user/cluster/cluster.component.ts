import { Component, OnInit } from "@angular/core";
import { Clusters } from "../../../type/clusters";
import { ClusterService } from "../../../../common/service/cluster/cluster.service";
import { Subscription } from "rxjs";
import { FormGroup } from "@angular/forms";
import { HttpErrorResponse } from "@angular/common/http";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-cluster",
  templateUrl: "./cluster.component.html",
  styleUrls: ["./cluster.component.scss"],
})
export class ClusterComponent implements OnInit {
  private subscriptions: Subscription[] = [];
  private intervalId: any;
  isClusterManagementVisible = false;
  clusterList: Clusters[] = [];

  constructor(private clusterService: ClusterService) {}

  ngOnInit(): void {
    this.getClusters();
    this.startClusterPolling();
  }

  private startClusterPolling(): void {
    this.intervalId = setInterval(() => {
      this.getClusters();
    }, 1000);
  }

  private stopClusterPolling(): void {
    if (this.intervalId) {
      clearInterval(this.intervalId);
    }
  }

  getClusters() {
    this.clusterService
      .getClusters()
      .pipe(untilDestroyed(this))
      .subscribe(
        clusters => (this.clusterList = clusters || []),
        (error: HttpErrorResponse) => console.error("Error fetching clusters", error)
      );
  }

  createCluster(formData: FormData) {
    this.clusterService
      .createCluster(formData)
      .pipe(untilDestroyed(this))
      .subscribe(
        response => console.log("Response: ", response),
        (error: HttpErrorResponse) => console.error("Error fetching clusters", error)
      );
  }

  deleteCluster(cluster: Clusters): void {
    this.clusterService
      .deleteCluster(cluster)
      .pipe(untilDestroyed(this))
      .subscribe(
        response => console.log("Response: ", response),
        (error: HttpErrorResponse) => console.error("Error fetching clusters", error)
      );
  }

  pauseCluster(cluster: Clusters): void {
    this.clusterService
      .pauseCluster(cluster)
      .pipe(untilDestroyed(this))
      .subscribe(
        response => console.log("Response: ", response),
        (error: HttpErrorResponse) => console.error("Error fetching clusters", error)
      );
  }

  resumeCluster(cluster: Clusters): void {
    this.clusterService
      .resumeCluster(cluster)
      .pipe(untilDestroyed(this))
      .subscribe(
        response => console.log("Response: ", response),
        (error: HttpErrorResponse) => console.error("Error fetching clusters", error)
      );
  }

  updateCluster(cluster: Clusters): void {
    this.clusterService
      .updateCluster(cluster)
      .pipe(untilDestroyed(this))
      .subscribe(
        response => console.log("Response: ", response),
        (error: HttpErrorResponse) => console.error("Error fetching clusters", error)
      );
  }

  submitCluster(clusterForm: FormGroup): void {
    const formData = new FormData();
    formData.append("Name", clusterForm.value.Name);
    formData.append("machineType", clusterForm.value.machineType);
    formData.append("numberOfMachines", clusterForm.value.numberOfMachines);
    this.createCluster(formData);
    this.closeClusterManagementModal();
  }

  openClusterManagementModal(): void {
    this.isClusterManagementVisible = true;
  }

  closeClusterManagementModal(): void {
    this.isClusterManagementVisible = false;
  }

  getBadgeStatus(status: string): string[]{
    switch(status){
      case "LAUNCHING":
      case "RESUMING":
        return ["play-circle", "orange"];
      case "PAUSING":
        return ["pause-circle", "orange"];
      case "LAUNCHED":
        return ["check-circle", "green"];
      case "PAUSED":
        return ["pause-circle", "gray"];
      case "TERMINATING":
        return ["minus-circle", "orange"];
      case "TERMINATED":
      case "FAILED":
        return ["minus-circle", "red"];
      default:
        return ["exclamation-circle", "gray"];
    }
  }
}
