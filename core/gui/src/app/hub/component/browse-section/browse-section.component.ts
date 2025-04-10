import { ChangeDetectorRef, Component, Input, OnInit } from "@angular/core";
import { DashboardEntry } from "../../../dashboard/type/dashboard-entry";
import { WorkflowPersistService } from "../../../common/service/workflow-persist/workflow-persist.service";
import { DatasetService } from "../../../dashboard/service/user/dataset/dataset.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import {
  DASHBOARD_HUB_WORKFLOW_RESULT_DETAIL,
  DASHBOARD_USER_WORKSPACE,
  DASHBOARD_USER_DATASET,
  DASHBOARD_HUB_DATASET_RESULT_DETAIL,
} from "../../../app-routing.constant";

@UntilDestroy()
@Component({
  selector: "texera-browse-section",
  templateUrl: "./browse-section.component.html",
  styleUrls: ["./browse-section.component.scss"],
})
export class BrowseSectionComponent implements OnInit {
  @Input() entities: DashboardEntry[] = [];
  @Input() sectionTitle: string = "";
  @Input() currentUid: number | undefined;

  defaultBackground: string = "../../../../../assets/card_background.jpg";
  protected readonly DASHBOARD_HUB_WORKFLOW_RESULT_DETAIL = DASHBOARD_HUB_WORKFLOW_RESULT_DETAIL;
  protected readonly DASHBOARD_USER_WORKSPACE = DASHBOARD_USER_WORKSPACE;
  protected readonly DASHBOARD_HUB_DATASET_RESULT_DETAIL = DASHBOARD_HUB_DATASET_RESULT_DETAIL;
  protected readonly DASHBOARD_USER_DATASET = DASHBOARD_USER_DATASET;
  entityRoutes: { [key: number]: string[] } = {};

  constructor(
    private workflowPersistService: WorkflowPersistService,
    private datasetService: DatasetService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.entities.forEach(entity => {
      this.initializeEntry(entity);
    });
  }

  ngOnChanges(): void {
    this.entities.forEach(entity => {
      this.initializeEntry(entity);
    });
  }

  private initializeEntry(entity: DashboardEntry): void {
    if (typeof entity.id !== "number") {
      return;
    }

    const entityId = entity.id;

    if (entity.type === "workflow") {
      this.workflowPersistService
        .getWorkflowOwners(entityId)
        .pipe(untilDestroyed(this))
        .subscribe((owners: number[]) => {
          if (this.currentUid !== undefined && owners.includes(this.currentUid)) {
            this.entityRoutes[entityId] = [this.DASHBOARD_USER_WORKSPACE, String(entityId)];
          } else {
            this.entityRoutes[entityId] = [this.DASHBOARD_HUB_WORKFLOW_RESULT_DETAIL, String(entityId)];
          }
          this.cdr.detectChanges();
        });
    } else if (entity.type === "dataset") {
      this.datasetService
        .getDatasetOwners(entityId)
        .pipe(untilDestroyed(this))
        .subscribe((owners: number[]) => {
          if (this.currentUid !== undefined && owners.includes(this.currentUid)) {
            this.entityRoutes[entityId] = [this.DASHBOARD_USER_DATASET, String(entityId)];
          } else {
            this.entityRoutes[entityId] = [this.DASHBOARD_HUB_DATASET_RESULT_DETAIL, String(entityId)];
          }
          this.cdr.detectChanges();
        });
    } else {
      throw new Error("Unexpected type in DashboardEntry.");
    }
  }
}
