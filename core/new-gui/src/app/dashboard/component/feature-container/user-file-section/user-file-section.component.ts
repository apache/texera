import { Component } from "@angular/core";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalFileAddComponent } from "./ngbd-modal-file-add/ngbd-modal-file-add.component";
import { UserFileService } from "../../../service/user-file/user-file.service";
import { DashboardUserFileEntry, UserFile } from "../../../type/dashboard-user-file-entry";
import { UserService } from "../../../../common/service/user/user.service";
import { NgbdModalUserFileShareAccessComponent } from "./ngbd-modal-file-share-access/ngbd-modal-user-file-share-access.component";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import { NotificationService } from "src/app/common/service/notification/notification.service";
import Fuse from "fuse.js";

@UntilDestroy()
@Component({
  selector: "texera-user-file-section",
  templateUrl: "./user-file-section.component.html",
  styleUrls: ["./user-file-section.component.scss"],
})
export class UserFileSectionComponent {
  constructor(
    private modalService: NgbModal,
    private userFileService: UserFileService,
    private userService: UserService,
    private notificationService: NotificationService
  ) {
    this.userFileService.refreshDashboardUserFileEntries();
  }

  public isEditingName: number[] = [];
  public userFileSearchValue: string = "";
  public filteredFilenames: Set<string> = new Set();
  public isTyping: boolean = false;
  public fuse = new Fuse([] as ReadonlyArray<DashboardUserFileEntry>, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["file.name"],
  });

  public openFileAddComponent() {
    this.modalService.open(NgbdModalFileAddComponent);
  }

  public searchInputOnChange(value: string): void {
    this.isTyping = true;
    this.filteredFilenames.clear();
    const fileArray = this.userFileService.getUserFiles();
    fileArray.forEach(fileEntry => {
      if (fileEntry.file.name.toLowerCase().indexOf(value.toLowerCase()) !== -1) {
        this.filteredFilenames.add(fileEntry.file.name);
      }
    });
  }

  public onClickOpenShareAccess(dashboardUserFileEntry: DashboardUserFileEntry): void {
    const modalRef = this.modalService.open(NgbdModalUserFileShareAccessComponent);
    modalRef.componentInstance.dashboardUserFileEntry = dashboardUserFileEntry;
  }

  public getFileArray(): ReadonlyArray<DashboardUserFileEntry> {
    const fileArray = this.userFileService.getUserFiles();
    if (!fileArray) {
      return [];
    } else if (this.userFileSearchValue !== "" && this.isTyping === false) {
      this.fuse.setCollection(fileArray);
      return this.fuse.search(this.userFileSearchValue).map(item => {
        return item.item;
      });
    }
    return fileArray;
  }

  public deleteUserFileEntry(userFileEntry: DashboardUserFileEntry): void {
    this.userFileService.deleteDashboardUserFileEntry(userFileEntry);
  }

  public disableAddButton(): boolean {
    return !this.userService.isLogin();
  }

  public addFileSizeUnit(fileSize: number): string {
    return this.userFileService.addFileSizeUnit(fileSize);
  }

  public downloadUserFile(userFileEntry: DashboardUserFileEntry): void {
    this.userFileService
      .downloadUserFile(userFileEntry.file)
      .pipe(untilDestroyed(this))
      .subscribe(
        (response: Blob) => {
          // prepare the data to be downloaded.
          const dataType = response.type;
          const binaryData = [];
          binaryData.push(response);

          // create a download link and trigger it.
          const downloadLink = document.createElement("a");
          downloadLink.href = URL.createObjectURL(new Blob(binaryData, { type: dataType }));
          downloadLink.setAttribute("download", userFileEntry.file.name);
          document.body.appendChild(downloadLink);
          downloadLink.click();
          URL.revokeObjectURL(downloadLink.href);
        },
        (err: unknown) => {
          // @ts-ignore // TODO: fix this with notification component
          this.message.error(err.error.message);
        }
      );
  }

  public confirmUpdateFileCustomName(
    dashboardUserFileEntry: DashboardUserFileEntry,
    name: string,
    index: number
  ): void {
    const {
      file: { fid },
    } = dashboardUserFileEntry;
    this.userFileService
      .updateFileName(fid, name)
      .pipe(untilDestroyed(this))
      .subscribe(
        () => this.userFileService.refreshDashboardUserFileEntries(),
        (err: unknown) => {
          // @ts-ignore // TODO: fix this with notification component
          this.notificationService.error(err.error.message);
          this.userFileService.refreshDashboardUserFileEntries();
        }
      )
      .add(() => (this.isEditingName = this.isEditingName.filter(fileIsEditing => fileIsEditing != index)));
  }
}
