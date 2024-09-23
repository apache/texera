import { Component, OnInit, OnDestroy } from "@angular/core";
import { Clusters } from "../../../type/clusters";
import { ClusterService } from "../../../../common/service/cluster/cluster.service";
import { FormGroup } from "@angular/forms";
import { HttpErrorResponse } from "@angular/common/http";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { BehaviorSubject, Observable, timer } from "rxjs";
import { switchMap, distinctUntilChanged, map } from "rxjs/operators";

@UntilDestroy()
@Component({
  selector: "texera-cluster",
  templateUrl: "./cluster.component.html",
  styleUrls: ["./cluster.component.scss"],
})
export class ClusterComponent implements OnInit, OnDestroy {
  isClusterManagementVisible = false;
  clusterList$!: Observable<Clusters[]>;
  private refreshTrigger = new BehaviorSubject<void>(undefined);
  pageSize = 10;
  pageIndex = 1;

  constructor(private clusterService: ClusterService) {}

  ngOnInit(): void {
    this.setupClusterObservable();
    this.refreshClusters();
  }

  ngOnDestroy(): void {
    this.refreshTrigger.complete();
  }

  private setupClusterObservable(): void {
    this.clusterList$ = this.refreshTrigger.pipe(
      switchMap(() => timer(0, 5000)),
      switchMap(() => this.clusterService.getClusters()),
      map(clusters => clusters || []),
      distinctUntilChanged((prev, curr) => JSON.stringify(prev) === JSON.stringify(curr)),
      untilDestroyed(this)
    );
  }

  refreshClusters(): void {
    this.refreshTrigger.next();
  }

  createCluster(formData: FormData) {
    this.clusterService
      .createCluster(formData)
      .pipe(untilDestroyed(this))
      .subscribe(
        response => {
          console.log("Response: ", response);
          this.refreshClusters();
        },
        (error: HttpErrorResponse) => console.error("Error creating cluster", error)
      );
  }

  deleteCluster(cluster: Clusters): void {
    this.clusterService
      .deleteCluster(cluster)
      .pipe(untilDestroyed(this))
      .subscribe(
        response => {
          console.log("Response: ", response);
          this.refreshClusters();
        },
        (error: HttpErrorResponse) => console.error("Error deleting cluster", error)
      );
  }

  pauseCluster(cluster: Clusters): void {
    this.clusterService
      .pauseCluster(cluster)
      .pipe(untilDestroyed(this))
      .subscribe(
        response => {
          console.log("Response: ", response);
          this.refreshClusters();
        },
        (error: HttpErrorResponse) => console.error("Error pausing cluster", error)
      );
  }

  resumeCluster(cluster: Clusters): void {
    this.clusterService
      .resumeCluster(cluster)
      .pipe(untilDestroyed(this))
      .subscribe(
        response => {
          console.log("Response: ", response);
          this.refreshClusters();
        },
        (error: HttpErrorResponse) => console.error("Error resuming cluster", error)
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

  getBadgeStatus(status: string): string[] {
    switch (status) {
      case "LAUNCHING":
      case "RESUMING":
        return ["loading", "green"];
      case "PAUSING":
        return ["loading", "orange"];
      case "LAUNCHED":
        return ["check-circle", "green"];
      case "PAUSED":
        return ["pause-circle", "gray"];
      case "TERMINATING":
        return ["loading", "red"];
      case "TERMINATED":
      case "FAILED":
        return ["minus-circle", "red"];
      default:
        return ["exclamation-circle", "gray"];
    }
  }
  getMachineTypeInfo(machineType: string): string {
    switch (machineType) {
      case "t2.micro":
        return "1 CPU, 1 GB RAM, $0.0116/hour";
      case "t3.large":
        return "2 CPUs, 8 GB RAM, $0.0832/hour";
      case "t3.xlarge":
        return "4 CPUs, 16 GB RAM, $0.1664/hour";
      case "t3.2xlarge":
        return "8 CPUs, 32 GB RAM, $0.3328/hour";
      default:
        return "Information not available";
    }
  }

  onPageIndexChange(index: number): void {
    this.pageIndex = index;
  }

  onPageSizeChange(size: number): void {
    this.pageSize = size;
    this.pageIndex = 1;
  }
}
