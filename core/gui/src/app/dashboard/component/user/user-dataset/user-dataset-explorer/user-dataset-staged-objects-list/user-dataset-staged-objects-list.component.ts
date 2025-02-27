import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { DatasetStagedObject } from "../../../../../../common/type/dataset-staged-object";
import { DatasetService } from "../../../../../service/user/dataset/dataset.service";
import { NotificationService } from "../../../../../../common/service/notification/notification.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";

@UntilDestroy()
@Component({
  selector: "texera-dataset-staged-objects-list",
  templateUrl: "./user-dataset-staged-objects-list.component.html",
  styleUrls: ["./user-dataset-staged-objects-list.component.scss"],
})
export class UserDatasetStagedObjectsListComponent implements OnInit {
  @Input() did?: number; // Dataset ID
  @Input() set userMakeChangesEvent(event: EventEmitter<void>) {
    if (event) {
      event.pipe(untilDestroyed(this)).subscribe(() => {
        this.fetchDatasetStagedObjects();
      });
    }
  }

  @Output() stagedObjectsChanged = new EventEmitter<DatasetStagedObject[]>(); // Emits staged objects list

  datasetStagedObjects: DatasetStagedObject[] = [];

  constructor(
    private datasetService: DatasetService,
    private notificationService: NotificationService
  ) {}

  ngOnInit(): void {
    this.fetchDatasetStagedObjects();
  }

  private fetchDatasetStagedObjects(): void {
    if (this.did != undefined) {
      this.datasetService
        .getDatasetDiff(this.did)
        .pipe(untilDestroyed(this))
        .subscribe(diffs => {
          this.datasetStagedObjects = diffs;
          // Emit the updated staged objects list
          this.stagedObjectsChanged.emit(this.datasetStagedObjects);
        });
    }
  }

  onObjectReverted(objDiff: DatasetStagedObject) {
    if (this.did) {
      this.datasetService
        .resetDatasetFileDiff(this.did, objDiff.path)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: (res: Response) => {
            this.notificationService.success(`"${objDiff.diffType} ${objDiff.path}" is successfully reverted`);
            this.fetchDatasetStagedObjects();
          },
          error: (err: unknown) => {
            this.notificationService.error("Failed to delete the file");
          },
        });
    }
  }
}
