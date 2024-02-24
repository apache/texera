import { Component, EventEmitter, Input, Output } from "@angular/core";
import { DashboardFile } from "../../../type/dashboard-file.interface";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NzModalService } from "ng-zorro-antd/modal";
import { UserFileService } from "../../../service/user-file/user-file.service";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import { ShareAccessComponent } from "../../share-access/share-access.component";

@UntilDestroy()
@Component({
  selector: "texera-user-file-list-item",
  templateUrl: "./user-file-list-item.component.html",
  styleUrls: ["./user-file-list-item.component.scss"],
})
export class UserFileListItemComponent {
  private _entry?: DashboardFile = {
    ownerEmail: "jingchf@uci.edu",
    accessLevel: "WRITE",
    file: {
      ownerUid: 1,
      fid: 2,
      size: 2854,
      name: "texera test.txt",
      path: "/home/vagrant/texera/core/amber/user-resources/files/1/essay1 (1).txt",
      description: "",
      uploadTime: 1682533263000,
    },
  };

  @Input() public keywords: string[] = [];
  @Input() get entry(): DashboardFile {
    if (!this._entry) {
      throw new Error("entry property must be set in UserFileListItemComponent.");
    }
    return this._entry;
  }
  set entry(value: DashboardFile) {
    this._entry = value;
  }
  @Input() editable = false;
  @Input() uid: number | undefined;
  editingName = false;
  editingDescription = false;
  @Output() deleted = new EventEmitter<void>();
  @Output() refresh = new EventEmitter<void>();

  constructor(
    private modalService: NzModalService,
    private userFileService: UserFileService,
    private notificationService: NotificationService
  ) {}

  public confirmUpdateFileCustomName(name: string): void {
    if (this.entry.file.name === name) {
      return;
    }
    this.userFileService
      .changeFileName(this.entry.file.fid, name)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => (this.entry.file.name = name),
        error: (e: unknown) => this.notificationService.error((e as Error).message),
      })
      .add(() => (this.editingName = false));
  }

  public confirmUpdateFileCustomDescription(description: string): void {
    this.userFileService
      .changeFileDescription(this.entry.file.fid, description)
      .pipe(untilDestroyed(this))
      .subscribe({
        next: () => (this.entry.file.description = description),
        error: (e: unknown) => this.notificationService.error((e as Error).message),
      })
      .add(() => (this.editingDescription = false));
  }

  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }

  public onClickOpenShareAccess(): void {
    const modalRef = this.modalService.create({
      nzContent: ShareAccessComponent,
      nzData: {
        writeAccess: this.entry.accessLevel === "WRITE",
        type: "file",
        id: this.entry.file.fid,
      },
      nzFooter: null,
      nzTitle: "Share this file with others",
      nzCentered: true,
    });
    modalRef.afterClose.pipe(untilDestroyed(this)).subscribe(() => this.refresh.emit());
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
