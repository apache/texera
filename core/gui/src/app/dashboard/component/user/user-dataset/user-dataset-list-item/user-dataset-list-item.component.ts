import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { Component, EventEmitter, Input, Output } from "@angular/core";
import { Dataset } from "../../../../../common/type/dataset";
import { DatasetService } from "../../../../service/user/dataset/dataset.service";
import { ShareAccessComponent } from "../../share-access/share-access.component";
import { NotificationService } from "../../../../../common/service/notification/notification.service";
import { NzModalService } from "ng-zorro-antd/modal";
import { DashboardDataset } from "../../../../type/dashboard-dataset.interface";

@UntilDestroy()
@Component({
  selector: "texera-user-dataset-list-item",
  templateUrl: "./user-dataset-list-item.component.html",
  styleUrls: ["./user-dataset-list-item.component.scss"],
})
export class UserDatasetListItemComponent {
  ROUTER_DATASET_BASE_URL = "/dashboard/user/dataset";

  private _entry?: DashboardDataset;

  @Output()
  refresh = new EventEmitter<void>();

  @Input()
  get entry(): DashboardDataset {
    if (!this._entry) {
      throw new Error("entry property must be provided to UserDatasetListItemComponent.");
    }
    return this._entry;
  }

  set entry(value: DashboardDataset) {
    this._entry = value;
  }

  get dataset(): Dataset {
    if (!this.entry.dataset) {
      throw new Error(
        "Incorrect type of DashboardEntry provided to UserDatasetListItemComponent. Entry must be dataset."
      );
    }
    return this.entry.dataset;
  }

  @Input() editable = false;
  @Output() deleted = new EventEmitter<void>();
  @Output() duplicated = new EventEmitter<void>();

  editingName = false;
  editingDescription = false;

  constructor(
    private modalService: NzModalService,
    private datasetService: DatasetService,
    private notificationService: NotificationService
  ) {}

  public confirmUpdateDatasetCustomName(name: string) {
    if (this.entry.dataset.name === name) {
      return;
    }

    if (this.entry.dataset.did)
      this.datasetService
        .updateDatasetName(this.entry.dataset.did, name)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.entry.dataset.name = name;
            this.editingName = false;
          },
          error: () => {
            this.notificationService.error("Update dataset name failed");
            this.editingName = false;
          },
        });
  }

  public confirmUpdateDatasetCustomDescription(description: string) {
    if (this.entry.dataset.description === description) {
      return;
    }

    if (this.entry.dataset.did)
      this.datasetService
        .updateDatasetDescription(this.entry.dataset.did, description)
        .pipe(untilDestroyed(this))
        .subscribe({
          next: () => {
            this.entry.dataset.description = description;
            this.editingDescription = false;
          },
          error: () => {
            this.notificationService.error("Update dataset description failed");
            this.editingDescription = false;
          },
        });
  }

  public onClickOpenShareAccess() {
    this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: this.entry.accessPrivilege === "WRITE",
        type: "dataset",
        id: this.dataset.did,
      },
      nzFooter: null,
      nzTitle: "Share this dataset with others",
      nzCentered: true,
    });
  }
}
