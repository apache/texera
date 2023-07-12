import { Component, OnInit } from "@angular/core";
import { NgbModal } from "@ng-bootstrap/ng-bootstrap";
import { NgbdModalFileAddComponent } from "./ngbd-modal-file-add/ngbd-modal-file-add.component";
import { UserFileService } from "../../service/user-file/user-file.service";
import { DashboardFile, SortMethod } from "../../type/dashboard-file.interface";
import { UserService } from "../../../../common/service/user/user.service";
import { UntilDestroy, untilDestroyed } from "@ngneat/until-destroy";
import Fuse from "fuse.js";

@UntilDestroy()
@Component({
  selector: "texera-user-file",
  templateUrl: "./user-file.component.html",
  styleUrls: ["./user-file.component.scss"],
})
export class UserFileComponent implements OnInit {
  // variables for file editing / search / sort
  public dashboardUserFileEntries: ReadonlyArray<DashboardFile> = [];
  public userFileSearchValue: string = "";
  public filteredFilenames: Array<string> = [];
  public isTyping: boolean = false;
  public fuse = new Fuse([] as ReadonlyArray<DashboardFile>, {
    shouldSort: true,
    threshold: 0.2,
    location: 0,
    distance: 100,
    minMatchCharLength: 1,
    keys: ["file.name"],
  });
  public sortMethod: SortMethod = SortMethod.UploadTimeDesc;
  public uid: number | undefined;

  constructor(
    private modalService: NgbModal,
    private userFileService: UserFileService,
    private userService: UserService
  ) {
    this.uid = this.userService.getCurrentUser()?.uid;
  }

  ngOnInit() {
    this.refreshDashboardFileEntries();
  }

  public openFileAddComponent() {
    const modalRef = this.modalService.open(NgbdModalFileAddComponent);

    modalRef.dismissed.pipe(untilDestroyed(this)).subscribe(_ => {
      this.refreshDashboardFileEntries();
    });
  }

  public searchInputOnChange(value: string): void {
    this.isTyping = true;
    this.filteredFilenames = [];
    const fileArray = this.dashboardUserFileEntries;
    fileArray.forEach(fileEntry => {
      if (fileEntry.file.name.toLowerCase().indexOf(value.toLowerCase()) !== -1) {
        this.filteredFilenames.push(fileEntry.file.name);
      }
    });
  }

  public getFileArray(): ReadonlyArray<DashboardFile> {
    this.sortFileEntries(); // default sorting
    const fileArray = this.dashboardUserFileEntries;
    if (!fileArray) {
      return [];
    } else if (this.userFileSearchValue !== "" && !this.isTyping) {
      this.fuse.setCollection(fileArray);
      return this.fuse.search(this.userFileSearchValue).map(item => {
        return item.item;
      });
    } else if (!this.isTyping) {
      return fileArray.slice();
    }
    return fileArray;
  }

  public deleteFile(fid: number): void {
    if (fid === undefined) {
      return;
    }
    this.userFileService
      .deleteFile(fid)
      .pipe(untilDestroyed(this))
      .subscribe(() => this.refreshDashboardFileEntries());
  }

  /**
   * Sort the files according to sortMethod variable
   */
  public sortFileEntries(): void {
    switch (this.sortMethod) {
      case SortMethod.NameAsc:
        this.ascSort();
        break;
      case SortMethod.NameDesc:
        this.dscSort();
        break;
      case SortMethod.SizeDesc:
        this.sizeSort();
        break;
      case SortMethod.UploadTimeAsc:
        this.timeSortAsc();
        break;
      case SortMethod.UploadTimeDesc:
        this.timeSortDesc();
        break;
    }
  }

  /**
   * sort the workflow by owner name + file name in ascending order
   */
  public ascSort(): void {
    this.sortMethod = SortMethod.NameAsc;
    this.dashboardUserFileEntries = this.dashboardUserFileEntries
      .slice()
      .sort((t1, t2) =>
        (t1.ownerEmail + t1.file.name).toLowerCase().localeCompare((t2.ownerEmail + t2.file.name).toLowerCase())
      );
  }

  /**
   * sort the file by owner name + file name in descending order
   */
  public dscSort(): void {
    this.sortMethod = SortMethod.NameDesc;
    this.dashboardUserFileEntries = this.dashboardUserFileEntries
      .slice()
      .sort((t1, t2) =>
        (t2.ownerEmail + t2.file.name).toLowerCase().localeCompare((t1.ownerEmail + t1.file.name).toLowerCase())
      );
  }

  /**
   * sort the file by size in descending order
   */
  public sizeSort(): void {
    this.sortMethod = SortMethod.SizeDesc;
    this.dashboardUserFileEntries = this.dashboardUserFileEntries
      .slice()
      .sort((left, right) =>
        left.file.size !== undefined && right.file.size !== undefined ? right.file.size - left.file.size : 0
      );
  }

  /**
   * sort the file by upload time in descending order
   */
  public timeSortDesc(): void {
    this.sortMethod = SortMethod.UploadTimeDesc;
    this.dashboardUserFileEntries = this.dashboardUserFileEntries
      .slice()
      .sort((left, right) =>
        left.file.uploadTime !== undefined && right.file.uploadTime !== undefined
          ? right.file.uploadTime - left.file.uploadTime
          : 0
      );
  }

  /**
   * sort the file by upload time in ascending order
   */
  public timeSortAsc(): void {
    this.sortMethod = SortMethod.UploadTimeAsc;
    this.dashboardUserFileEntries = this.dashboardUserFileEntries
      .slice()
      .sort((left, right) =>
        left.file.uploadTime !== undefined && right.file.uploadTime !== undefined
          ? left.file.uploadTime - right.file.uploadTime
          : 0
      );
  }

  private refreshDashboardFileEntries(): void {
    this.userFileService
      .getFileList()
      .pipe(untilDestroyed(this))
      .subscribe(dashboardUserFileEntries => {
        this.dashboardUserFileEntries = dashboardUserFileEntries;
      });
  }
}
