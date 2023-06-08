import { Component, EventEmitter, Input, OnInit, Output } from "@angular/core";
import { DashboardFile } from "../../../type/dashboard-file.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { UserFileService } from "../../../service/user-file/user-file.service";
import { UserService } from "src/app/common/service/user/user.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { ShareAccessComponent } from "../../share-access/share-access.component";

@UntilDestroy()
@Component({
  selector: "texera-user-file-list-item",
  templateUrl: "./user-file-list-item.component.html",
  styleUrls: ["./user-file-list-item.component.scss"],
})
export class UserFileListItemComponent {
  public uid: number | undefined;
  private _entry?: DashboardFile;
  @Input() get entry(): DashboardFile {
    if (!this._entry) {
      throw new Error("entry property must be set in UserFileListItemComponent.");
    }
    return this._entry;
  }
  @Input() editable = false;
  editingName = false;
  editingDescription = false;
  set entry(value: DashboardFile) {
    this._entry = value;
  }
  @Output() deleted = new EventEmitter<void>();

  constructor(
    private modalService: NgbModal,
    private userFileService: UserFileService,
    private userService: UserService,
    private notificationService: NotificationService
  ) {
    this.uid = this.userService.getCurrentUser()?.uid;
  }

  public confirmUpdateFileCustomName(name: string): void {
    if (this.entry.file.name === name) {
      return;
    }
    this.userFileService
      .changeFileName(this.entry.file.fid, name)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => {
          this.entry.file.name = name;
        },
        error: (err: unknown) => {
          // @ts-ignore // TODO: fix this with notification component
          this.notificationService.error(err.error.message);
        },
      })
      .add(() => (this.editingName = false));
  }

  public confirmUpdateFileCustomDescription(description: string): void {
    const {
      file: { fid },
    } = this.entry;
    this.userFileService
      .changeFileDescription(fid, description)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => (this.entry.file.description = description),
        error: (err: unknown) => {
          // @ts-ignore
          this.notificationService.error(err.error.message);
        },
      })
      .add(() => (this.editingDescription = false));
  }

  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }

  public onClickOpenShareAccess(dashboardUserFileEntry: DashboardFile): void {
    const modalRef = this.modalService.open(ShareAccessComponent);
    modalRef.componentInstance.type = "file";
    modalRef.componentInstance.id = dashboardUserFileEntry.file.fid;
  }

  public downloadFile(): void {
    this.userFileService
      .downloadFile(this.entry.file.fid)
      .pipe(untilDestroyed(this))
      .subscribe((response: Blob) => {
        const link = document.createElement("a");
        link.download = this.entry.file.name;
        link.href = URL.createObjectURL(new Blob([response]));
        link.click();
      });
  }
}
